/**
 * Recovery: error isolation inside a tick, and what a restart does to the work that was in flight.
 *
 * Hermetic, like the other suites: a standalone engine over a throwaway SQLite file, fake enumerators, and a fake
 * execution client. A "restart" is a second engine opened on the SAME file with a fresh client, whose ref counter
 * starts again at x1 exactly like a consumer's in-process client after a reboot. Proves:
 *   - one group's enumerator throwing does not stop another group being fed;
 *   - a completion resolves only a live task, so a Done row from before the restart carrying the same ref cannot
 *     capture it;
 *   - reconcile re-drives an interrupted classic unit from the unit stored on its row (once, not double-counted),
 *     dead-letters it at the attempt cap, and holds its group open until it has run;
 *   - a walk claim orphaned between claiming a node and reserving its task is released, so the walk drains.
 */

const libPath = require('path');
const libOS = require('os');
const libFS = require('fs');
const Chai = require('chai');
const Expect = Chai.expect;

const { createStandalone } = require('../source/Operation-Queue.js');

function _engine(pFile, pWindowSize)
{
	return new Promise((fResolve, fReject) =>
		createStandalone({ SQLiteFilePath: pFile, WindowSize: pWindowSize }, (pError, pEngine) => (pError ? fReject(pError) : fResolve(pEngine))));
}

// Units carry Params, as a real consumer's do, so a re-drive can be checked for fidelity.
function pagingEnumerator(pAllKeys)
{
	return (pGroup, pCursorToken, pLimit) =>
	{
		let tmpOffset = pCursorToken ? parseInt(pCursorToken, 10) : 0;
		let tmpSlice = pAllKeys.slice(tmpOffset, tmpOffset + pLimit);
		let tmpNext = tmpOffset + tmpSlice.length;
		return Promise.resolve(
			{
				Units: tmpSlice.map((pKey) => ({ UnitKey: pKey, Params: { URL: 'https://example.com/' + pKey } })),
				NextCursorToken: String(tmpNext),
				Exhausted: tmpNext >= pAllKeys.length
			});
	};
}

// A client whose ref counter starts at x1 (a fresh one per "boot") and records every unit it is handed.
function recordingClient()
{
	let tmpSeq = 0;
	let tmpHanded = [];
	return {
		Handed: tmpHanded,
		enqueue: (pUnit) => { tmpHanded.push(pUnit); return Promise.resolve('x' + (++tmpSeq)); },
		cancel: () => Promise.resolve()
	};
}

suite('retold-operation-queue recovery', () =>
{
	let _dir;
	let _file;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(() =>
	{
		_dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-recover-'));
		_file = libPath.join(_dir, 'oq.sqlite');
	});
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	async function _activeGroup(pEngine, pSpec)
	{
		let tmpGroup = await pEngine.planGroup(_scope, Object.assign({ Label: 'g', Lane: 'Normal' }, pSpec));
		return pEngine.activateGroup(_scope, tmpGroup);
	}

	function _tasks(pEngine, pGroup)
	{
		return pEngine.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('IDOperationGroup', pGroup.IDOperationGroup));
	}

	// Report every live task of a group as a success.
	async function _completeLive(pEngine, pGroup)
	{
		let tmpLive = await pEngine.Store.readInflightTasksForGroup(_scope, pGroup.IDOperationGroup);
		for (let tmpTask of tmpLive)
		{
			await pEngine.Scheduler.reportOutcome(tmpTask.ExecRef, { Success: true });
		}
		return tmpLive.length;
	}

	test('A THROWING ENUMERATOR NO LONGER STOPS ANOTHER GROUP from being fed', async () =>
	{
		let tmpEngine = await _engine(_file, 4);
		let tmpClient = recordingClient();
		tmpEngine.Scheduler.setExecutionClient(tmpClient);
		tmpEngine.registerEnumerator('broken.op', () => { throw new Error('source unreadable'); });
		tmpEngine.registerEnumerator('good.op', pagingEnumerator([ 'a', 'b' ]));
		// The broken group is Urgent, so it is reached FIRST. Its throw used to reject the whole tick, and the good
		// group behind it was never fed -- on this tick or any later one.
		let tmpBroken = await _activeGroup(tmpEngine, { OperationType: 'broken.op', Lane: 'Urgent' });
		let tmpGood = await _activeGroup(tmpEngine, { OperationType: 'good.op', Lane: 'Background' });

		let tmpResult = await tmpEngine.Scheduler.tick();

		Expect(tmpClient.Handed.map((pUnit) => pUnit.UnitKey)).to.deep.equal([ 'a', 'b' ]);
		Expect(tmpResult.Fed).to.equal(2);
		Expect(tmpResult.Errors.map((pError) => pError.IDOperationGroup)).to.deep.equal([ tmpBroken.IDOperationGroup ]);
		Expect((await tmpEngine.Store.readGroup(_scope, tmpBroken.IDOperationGroup)).Status, 'the broken group stays Active to retry').to.equal('Active');

		// ...and the good group runs to Done around it.
		await _completeLive(tmpEngine, tmpGood);
		await tmpEngine.Scheduler.tick();
		Expect((await tmpEngine.Store.readGroup(_scope, tmpGood.IDOperationGroup)).Status).to.equal('Done');
	});

	test('A REF REUSED AFTER A RESTART RESOLVES THE LIVE TASK, not the finished row that carried it first', async () =>
	{
		let tmpFirst = await _engine(_file, 2);
		tmpFirst.Scheduler.setExecutionClient(recordingClient());
		tmpFirst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c' ]));
		let tmpGroup = await _activeGroup(tmpFirst, { OperationType: 'test.op' });
		await tmpFirst.Scheduler.tick();                   // a -> x1, b -> x2
		await _completeLive(tmpFirst, tmpGroup);           // both Done, still carrying x1 / x2

		// Restart: a new engine on the same file, and a client whose counter starts again at x1.
		let tmpSecond = await _engine(_file, 2);
		tmpSecond.Scheduler.setExecutionClient(recordingClient());
		tmpSecond.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c' ]));
		await tmpSecond.Scheduler.reconcileInflight();
		await tmpSecond.Scheduler.tick();                  // c -> x1, again

		let tmpC = await tmpSecond.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'c');
		Expect(tmpC.ExecRef).to.equal('x1');
		let tmpResolved = await tmpSecond.Scheduler.reportOutcome('x1', { Success: true });

		// The lookup used to return a's Done row (the older x1), so c stayed Fed forever and the group never reaped.
		Expect(tmpResolved, 'the completion landed on c').to.equal(tmpC.IDOperationTask);
		Expect((await tmpSecond.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'c')).Status).to.equal('Done');
		let tmpReload = await tmpSecond.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.CompletedUnits, 'each unit counted once').to.equal(3);
		Expect(tmpReload.Status).to.equal('Done');
	});

	test('RECONCILE RE-DRIVES AN INTERRUPTED CLASSIC UNIT from its stored unit -- once, and without double-counting it', async () =>
	{
		let tmpFirst = await _engine(_file, 2);
		tmpFirst.Scheduler.setExecutionClient(recordingClient());
		tmpFirst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c' ]));
		let tmpGroup = await _activeGroup(tmpFirst, { OperationType: 'test.op' });
		await tmpFirst.Scheduler.tick();                   // a, b in flight; the cursor is already past both
		// Crash: neither completes.

		let tmpSecond = await _engine(_file, 2);
		let tmpClient = recordingClient();
		tmpSecond.Scheduler.setExecutionClient(tmpClient);
		tmpSecond.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c' ]));
		Expect(await tmpSecond.Scheduler.reconcileInflight(3)).to.deep.equal({ Requeued: 2, DeadLettered: 0 });
		Expect((await _tasks(tmpSecond, tmpGroup)).map((pTask) => pTask.Status)).to.deep.equal([ 'Requeued', 'Requeued' ]);

		// The next tick re-feeds them ahead of new cursor work, with the Params they were first handed, marked as a
		// second attempt. They used to retire Failed and were lost: nothing would ever offer them again.
		await tmpSecond.Scheduler.tick();
		Expect(tmpClient.Handed.map((pUnit) => pUnit.UnitKey)).to.deep.equal([ 'a', 'b' ]);
		Expect(tmpClient.Handed[0].Params).to.deep.equal({ URL: 'https://example.com/a' });
		Expect(tmpClient.Handed.every((pUnit) => pUnit.Attempt === 1)).to.equal(true);
		Expect((await tmpSecond.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).FedUnits, 'a re-drive is the same unit, not a new one').to.equal(2);

		await _completeLive(tmpSecond, tmpGroup);
		await tmpSecond.Scheduler.tick();                  // c
		Expect(tmpClient.Handed.map((pUnit) => pUnit.UnitKey)).to.deep.equal([ 'a', 'b', 'c' ]);
		Expect(tmpClient.Handed[2].Attempt, 'a first feed carries no attempt').to.equal(undefined);
		await _completeLive(tmpSecond, tmpGroup);

		let tmpReload = await tmpSecond.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.Status).to.equal('Done');
		Expect(tmpReload.FedUnits).to.equal(3);
		Expect(tmpReload.CompletedUnits).to.equal(3);
		Expect(tmpReload.FailedUnits).to.equal(0);
		Expect((await _tasks(tmpSecond, tmpGroup)).every((pTask) => !pTask.Params), 'a Done row does not keep its stored unit').to.equal(true);
	});

	test('a classic unit that keeps getting interrupted dead-letters at the attempt cap instead of looping', async () =>
	{
		let tmpEngine = await _engine(_file, 2);
		tmpEngine.Scheduler.setExecutionClient(recordingClient());
		tmpEngine.registerEnumerator('test.op', pagingEnumerator([ 'a' ]));
		let tmpGroup = await _activeGroup(tmpEngine, { OperationType: 'test.op' });

		await tmpEngine.Scheduler.tick();
		Expect(await tmpEngine.Scheduler.reconcileInflight(2)).to.deep.equal({ Requeued: 1, DeadLettered: 0 });
		await tmpEngine.Scheduler.tick();
		Expect(await tmpEngine.Scheduler.reconcileInflight(2)).to.deep.equal({ Requeued: 1, DeadLettered: 0 });
		await tmpEngine.Scheduler.tick();
		Expect(await tmpEngine.Scheduler.reconcileInflight(2)).to.deep.equal({ Requeued: 0, DeadLettered: 1 });

		let tmpTask = await tmpEngine.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'a');
		Expect(tmpTask.Status).to.equal('Failed');
		Expect(tmpTask.Attempt).to.equal(3);
		Expect(tmpTask.LastError).to.match(/dead-lettered after 2 attempts/);

		await tmpEngine.Scheduler.tick();                  // nothing left to run: the group reaps
		let tmpReload = await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.FailedUnits).to.equal(1);
		Expect(tmpReload.Status).to.equal('Done');
	});

	test('A REQUEUED UNIT HOLDS ITS GROUP OPEN until it has run, even with its cursor exhausted and the window busy', async () =>
	{
		let tmpEngine = await _engine(_file, 1);
		tmpEngine.Scheduler.setExecutionClient(recordingClient());
		tmpEngine.registerEnumerator('test.op', pagingEnumerator([ 'a' ]));
		tmpEngine.registerEnumerator('busy.op', pagingEnumerator([ 'y' ]));
		let tmpGroup = await _activeGroup(tmpEngine, { OperationType: 'test.op', Lane: 'Background' });
		await tmpEngine.Scheduler.tick();                  // a in flight; the cursor is exhausted
		await tmpEngine.Scheduler.reconcileInflight();     // restart: a is Requeued

		// An Urgent group takes the only slot, so a cannot be re-fed this tick. The reap pass still visits a's group,
		// which has nothing in flight and an exhausted cursor: it used to be reaped Done with a never run.
		let tmpBusy = await _activeGroup(tmpEngine, { OperationType: 'busy.op', Lane: 'Urgent' });
		await tmpEngine.Scheduler.tick();
		Expect((await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Active');

		await _completeLive(tmpEngine, tmpBusy);
		await tmpEngine.Scheduler.tick();                  // the slot is free: a is re-fed
		Expect((await tmpEngine.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'a')).Status).to.equal('Fed');
		await _completeLive(tmpEngine, tmpGroup);
		Expect((await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Done');
	});

	test('a legacy in-flight row with no stored unit cannot be re-driven, so it retires Failed with the reason', async () =>
	{
		let tmpEngine = await _engine(_file, 2);
		tmpEngine.Scheduler.setExecutionClient(recordingClient());
		tmpEngine.registerEnumerator('test.op', pagingEnumerator([ 'a' ]));
		let tmpGroup = await _activeGroup(tmpEngine, { OperationType: 'test.op' });
		// A row reserved by 0.5.1, which did not store the unit.
		await tmpEngine.Store.createTask(_scope, { IDOperationGroup: tmpGroup.IDOperationGroup, UnitKey: 'old', Status: 'Fed', ExecRef: 'x9' });

		Expect(await tmpEngine.Scheduler.reconcileInflight()).to.deep.equal({ Requeued: 0, DeadLettered: 1 });
		let tmpTask = await tmpEngine.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'old');
		Expect(tmpTask.Status).to.equal('Failed');
		Expect(tmpTask.LastError).to.match(/no stored unit/);
		Expect((await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).FailedUnits).to.equal(1);
	});

	test('A WALK CLAIM ORPHANED BEFORE ITS TASK WAS RESERVED is released on reconcile, so the walk can drain', async () =>
	{
		let tmpEngine = await _engine(_file, 2);
		tmpEngine.Scheduler.setExecutionClient(recordingClient());
		let tmpGroup = await _activeGroup(tmpEngine, { OperationType: 'index', Lane: 'Background', Generative: true });

		// nextBatch claims the root node 'walking'; the crash comes before tick reserves its task row.
		let tmpBatch = await tmpEngine.nextBatch(_scope, tmpGroup, 2);
		Expect(tmpBatch.Units.map((pUnit) => pUnit.UnitKey)).to.deep.equal([ '' ]);
		Expect(await _tasks(tmpEngine, tmpGroup)).to.have.length(0);

		// Nothing would ever complete that node, so the frontier never emptied and the group never reaped.
		await tmpEngine.Scheduler.tick();
		Expect(await tmpEngine.Store.countInflightTasks(), 'a tick cannot see the stranded claim').to.equal(0);
		Expect((await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Active');

		Expect(await tmpEngine.Scheduler.reconcileInflight()).to.deep.equal({ Requeued: 1, DeadLettered: 0 });
		await tmpEngine.Scheduler.tick();
		Expect(await _completeLive(tmpEngine, tmpGroup)).to.equal(1);
		Expect((await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Done');
	});
});
