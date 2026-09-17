/**
 * Feed durability: the hand-off from the plan to the execution queue.
 *
 * The scheduler used to enqueue a unit and THEN write its task row. Two windows had no way back, and both
 * were silent:
 *
 *   - enqueue REJECTS. The exception left tick() entirely, so every remaining unit in the batch was
 *     dropped permanently, because nextBatch persists the cursor past a unit before it is ever handed over.
 *     One transient failure from the execution queue lost the rest of the batch with no trace.
 *   - enqueue SUCCEEDS and the row write fails. The work runs in the execution queue with no task row, so
 *     neither the completion path (which resolves by ExecRef) nor reconcile can ever see it.
 *
 * The row is now reserved BEFORE the hand-off. An orphaned execution is undetectable; an orphaned row is
 * not. These pin that inversion, and each is paired with a mutation check in the work item.
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

function pagingEnumerator(pAllKeys)
{
	return (pGroup, pCursorToken, pLimit) =>
	{
		let tmpOffset = pCursorToken ? parseInt(pCursorToken, 10) : 0;
		let tmpSlice = pAllKeys.slice(tmpOffset, tmpOffset + pLimit);
		let tmpNext = tmpOffset + tmpSlice.length;
		return Promise.resolve({ Units: tmpSlice.map((pKey) => ({ UnitKey: pKey })), NextCursorToken: String(tmpNext), Exhausted: tmpNext >= pAllKeys.length });
	};
}

// A client that fails a chosen unit and records everything it was actually handed.
function flakyClient(pFailKeys)
{
	let tmpSeq = 0;
	let tmpFail = new Set(pFailKeys || []);
	let tmpHandled = [];
	return {
		Handled: tmpHandled,
		enqueue: (pUnit) =>
		{
			if (tmpFail.has(pUnit.UnitKey)) { return Promise.reject(new Error('execution queue said no')); }
			tmpHandled.push(pUnit.UnitKey);
			return Promise.resolve('x' + (++tmpSeq));
		},
		cancel: () => Promise.resolve()
	};
}

suite('retold-operation-queue feed durability', () =>
{
	let _engineInst;
	let _dir;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(async () =>
	{
		_dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-feed-'));
		_engineInst = await _engine(libPath.join(_dir, 'oq.sqlite'), 5);
	});
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	async function _activeGroup(pOperationType)
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: pOperationType, Label: 'g', Lane: 'Normal' });
		return _engineInst.activateGroup(_scope, tmpGroup);
	}

	test('A FAILING HAND-OFF NO LONGER STRANDS THE REST OF THE BATCH', async () =>
	{
		// 'b' fails. Before this, the rejection left tick() and 'c' through 'e' were never fed -- and the
		// cursor had already moved past all of them, so they were lost for good.
		let tmpClient = flakyClient([ 'b' ]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd', 'e' ]));
		await _activeGroup('test.op');

		let tmpResult = await _engineInst.Scheduler.tick();

		Expect(tmpClient.Handled).to.deep.equal([ 'a', 'c', 'd', 'e' ]);
		Expect(tmpResult.Fed, 'four of five units were handed over').to.equal(4);
	});

	test('the tick does not throw when the execution queue rejects', async () =>
	{
		let tmpClient = flakyClient([ 'a', 'b', 'c' ]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c' ]));
		await _activeGroup('test.op');

		let tmpResult = await _engineInst.Scheduler.tick();
		Expect(tmpResult.Fed).to.equal(0);
	});

	test('A UNIT WHOSE HAND-OFF FAILED KEEPS A ROW, so reconcile can find it', async () =>
	{
		// The row is the whole point. Without it the unit is invisible: an orphaned execution cannot be
		// detected, but an orphaned row can.
		let tmpClient = flakyClient([ 'b' ]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b' ]));
		let tmpGroup = await _activeGroup('test.op');

		await _engineInst.Scheduler.tick();

		let tmpTask = await _engineInst.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'b');
		Expect(tmpTask, 'a row exists for the unit that was never dispatched').to.not.equal(null);
		Expect(tmpTask.Status).to.equal('Fed');
		Expect(String(tmpTask.ExecRef || ''), 'and it carries no handle, which is how it is recognised').to.equal('');
	});

	test('RECONCILE SEES A RESERVED ROW -- it used to require an ExecRef and skip exactly these', async () =>
	{
		let tmpClient = flakyClient([ 'b' ]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b' ]));
		let tmpGroup = await _activeGroup('test.op');
		await _engineInst.Scheduler.tick();

		let tmpOrphans = await _engineInst.Store.readInflightTasks();
		Expect(tmpOrphans.map((pRow) => pRow.UnitKey).sort()).to.deep.equal([ 'a', 'b' ]);

		let tmpReconciled = await _engineInst.Scheduler.reconcileInflight();
		Expect(tmpReconciled.Requeued + tmpReconciled.DeadLettered, 'both in-flight rows were dealt with').to.equal(2);

		// The reserved row carries its unit, so reconcile re-drives it (see Operation-Queue-Recovery_tests) rather than
		// leaving it Fed or throwing the unit away.
		let tmpAfter = await _engineInst.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'b');
		Expect(tmpAfter.Status, 'the reserved row is queued to be re-driven rather than lingering Fed').to.equal('Requeued');
	});

	test('a successful hand-off records the handle, so completion can still resolve it', async () =>
	{
		let tmpClient = flakyClient([]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a' ]));
		let tmpGroup = await _activeGroup('test.op');

		await _engineInst.Scheduler.tick();

		let tmpTask = await _engineInst.Store.readTaskByUnitKey(_scope, tmpGroup.IDOperationGroup, 'a');
		Expect(tmpTask.ExecRef).to.equal('x1');
		Expect(await _engineInst.Store.readTaskByExecRef('x1'), 'and it is findable by handle').to.not.equal(null);
	});

	test('RE-FEEDING A UNIT REUSES ITS ROW rather than opening a second one for the same work', async () =>
	{
		// A cursor that offers a unit twice after a crash must not produce two rows, or the window is
		// occupied twice by one piece of work and the group can never reap.
		let tmpClient = flakyClient([ 'a' ]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', (pGroup, pCursorToken, pLimit) =>
			Promise.resolve({ Units: [ { UnitKey: 'a' } ], NextCursorToken: '', Exhausted: false }));
		let tmpGroup = await _activeGroup('test.op');

		await _engineInst.Scheduler.tick();
		await _engineInst.Scheduler.tick();
		await _engineInst.Scheduler.tick();

		let tmpAll = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('IDOperationGroup', tmpGroup.IDOperationGroup));
		Expect(tmpAll.filter((pRow) => pRow.UnitKey === 'a')).to.have.length(1);
	});

	test('a unit already in flight is left alone by a later tick', async () =>
	{
		let tmpClient = flakyClient([]);
		_engineInst.Scheduler.setExecutionClient(tmpClient);
		_engineInst.registerEnumerator('test.op', (pGroup, pCursorToken, pLimit) =>
			Promise.resolve({ Units: [ { UnitKey: 'a' } ], NextCursorToken: '', Exhausted: false }));
		await _activeGroup('test.op');

		await _engineInst.Scheduler.tick();
		let tmpHandledAfterFirst = tmpClient.Handled.length;
		await _engineInst.Scheduler.tick();

		Expect(tmpClient.Handled.length, 'the second tick did not dispatch it again').to.equal(tmpHandledAfterFirst);
	});
});
