/**
 * retold-operation-queue scheduler: Phase-C tests.
 *
 * Hermetic. Each test boots a standalone engine over a throwaway SQLite file, registers a fake
 * enumerator, and injects a fake execution client that records what it was handed. Proves: the
 * bounded window fills and no further (backpressure), higher-priority groups feed first, completion
 * frees a slot so the next tick refills, failures are recorded, and a whole group cancels in O(window).
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

// A fake execution client: hands back a unique ref per enqueue and tracks which refs are still live.
function fakeClient()
{
	let tmpSeq = 0;
	let tmpLive = new Map();
	return {
		enqueue: (pUnit) => { let tmpRef = 'x' + (++tmpSeq); tmpLive.set(tmpRef, pUnit); return Promise.resolve(tmpRef); },
		cancel: (pRef) => { tmpLive.delete(pRef); return Promise.resolve(); },
		live: () => Array.from(tmpLive.keys())
	};
}

suite('retold-operation-queue scheduler', () =>
{
	let _engineInst;
	let _client;
	let _dir;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(async () =>
	{
		_dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-sched-'));
		_engineInst = await _engine(libPath.join(_dir, 'oq.sqlite'), 3);
		_client = fakeClient();
		_engineInst.Scheduler.setExecutionClient(_client);
	});
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	async function _activeGroup(pOperationType, pLane)
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: pOperationType, Label: 'g', Lane: pLane });
		return _engineInst.activateGroup(_scope, tmpGroup);
	}

	test('tick fills the window up to WindowSize, then backpressures', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd', 'e', 'f' ]));
		await _activeGroup('test.op', 'Normal');

		let tmpFirst = await _engineInst.Scheduler.tick();
		Expect(tmpFirst.Fed).to.equal(3);
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(3);
		Expect(_client.live().length).to.equal(3);

		// window is full -> the next tick feeds nothing
		let tmpSecond = await _engineInst.Scheduler.tick();
		Expect(tmpSecond.Fed).to.equal(0);
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(3);
	});

	test('tick feeds a higher-priority (Urgent) group before a Background one', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd' ]));
		let tmpBackground = await _activeGroup('test.op', 'Background');
		let tmpUrgent = await _activeGroup('test.op', 'Urgent');

		await _engineInst.Scheduler.tick(); // WindowSize 3
		let tmpTasks = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
		// every fed task belongs to the Urgent group (it is drained first; window is 3, it had 4 ready)
		Expect(tmpTasks.length).to.equal(3);
		Expect(tmpTasks.every((pTask) => pTask.IDOperationGroup === tmpUrgent.IDOperationGroup)).to.equal(true);
		Expect(tmpTasks.some((pTask) => pTask.IDOperationGroup === tmpBackground.IDOperationGroup)).to.equal(false);
	});

	test('reportOutcome success frees a slot, bumps CompletedUnits, and the next tick refills', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd' ]));
		let tmpGroup = await _activeGroup('test.op', 'Normal');

		await _engineInst.Scheduler.tick(); // fills 3
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(3);

		let tmpTasks = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
		await _engineInst.Scheduler.reportOutcome(tmpTasks[0].ExecRef, { Success: true });
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(2);

		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.CompletedUnits).to.equal(1);

		// a slot freed -> next tick refills back to the window size (one more: 'd')
		let tmpRefill = await _engineInst.Scheduler.tick();
		Expect(tmpRefill.Fed).to.equal(1);
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(3);
	});

	test('reportOutcome failure marks the task Failed and bumps FailedUnits without occupying the window', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b' ]));
		let tmpGroup = await _activeGroup('test.op', 'Normal');
		await _engineInst.Scheduler.tick();

		let tmpTasks = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
		await _engineInst.Scheduler.reportOutcome(tmpTasks[0].ExecRef, { Success: false, Error: 'boom' });

		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.FailedUnits).to.equal(1);
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(1); // the failed one no longer occupies

		let tmpFailed = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Failed'));
		Expect(tmpFailed.length).to.equal(1);
		Expect(tmpFailed[0].LastError).to.equal('boom');
	});

	test('cancelGroup cancels the in-flight tasks and marks the group Canceled', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd' ]));
		let tmpGroup = await _activeGroup('test.op', 'Normal');
		await _engineInst.Scheduler.tick(); // 3 in-flight
		Expect(_client.live().length).to.equal(3);

		let tmpCanceled = await _engineInst.Scheduler.cancelGroup(_scope, tmpGroup);
		Expect(tmpCanceled).to.equal(3);
		Expect(_client.live().length).to.equal(0); // client.cancel was called for each
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(0);

		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.Status).to.equal('Canceled');
	});
});
