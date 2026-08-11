/**
 * retold-operation-queue Ultravisor execution client: Phase-D tests.
 *
 * Hermetic. A fake fable-ultravisor-client (an object with request(method, path, body, cb)) stands in for
 * a real Ultravisor coordinator, recording calls and returning canned responses. Proves the adapter maps
 * a unit to an enqueue body + returns the work-item hash, cancels by hash, errors on a hashless response,
 * and -- driven through the real scheduler -- feeds Ultravisor and reconciles completions back.
 */

const libPath = require('path');
const libOS = require('os');
const libFS = require('fs');
const Chai = require('chai');
const Expect = Chai.expect;

const { createStandalone, UltravisorExecutionClient } = require('../source/Operation-Queue.js');

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

// A fake fable-ultravisor-client: records requests, returns a unique WorkItemHash per enqueue.
function fakeUltravisorClient()
{
	let tmpSeq = 0;
	let tmpCalls = [];
	return {
		Calls: tmpCalls,
		request: (pMethod, pPath, pBody, fCallback) =>
		{
			tmpCalls.push({ Method: pMethod, Path: pPath, Body: pBody });
			if (pPath === '/Beacon/Work/Enqueue') { return fCallback(null, { WorkItemHash: 'wi-' + (++tmpSeq) }); }
			if (/\/Cancel$/.test(pPath)) { return fCallback(null, { Success: true }); }
			return fCallback(null, {});
		}
	};
}

suite('retold-operation-queue Ultravisor execution client', () =>
{
	let _dir;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(() => { _dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-uvc-')); });
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	test('enqueue maps the unit to a work-item body and returns the WorkItemHash', async () =>
	{
		let tmpClient = fakeUltravisorClient();
		let tmpExec = new UltravisorExecutionClient({ Client: tmpClient });

		let tmpRef = await tmpExec.enqueue({ UnitKey: 'photo.jpg', Priority: 500, Params: { Op: 'thumbnail', Path: '2/photo.jpg' } });
		Expect(tmpRef).to.equal('wi-1');

		let tmpEnqueue = tmpClient.Calls.find((pCall) => pCall.Path === '/Beacon/Work/Enqueue');
		Expect(tmpEnqueue.Method).to.equal('POST');
		Expect(tmpEnqueue.Body.Priority).to.equal(500);
		Expect(tmpEnqueue.Body.Settings).to.deep.equal({ Op: 'thumbnail', Path: '2/photo.jpg' });
	});

	test('a custom mapUnit overrides the default body', async () =>
	{
		let tmpClient = fakeUltravisorClient();
		let tmpExec = new UltravisorExecutionClient({ Client: tmpClient, MapUnit: (pUnit) => ({ Capability: 'Rendition', Action: 'Thumbnail', Priority: pUnit.Priority, Settings: { Key: pUnit.UnitKey } }) });

		await tmpExec.enqueue({ UnitKey: 'x.png', Priority: 900 });
		let tmpEnqueue = tmpClient.Calls.find((pCall) => pCall.Path === '/Beacon/Work/Enqueue');
		Expect(tmpEnqueue.Body.Capability).to.equal('Rendition');
		Expect(tmpEnqueue.Body.Action).to.equal('Thumbnail');
		Expect(tmpEnqueue.Body.Settings).to.deep.equal({ Key: 'x.png' });
	});

	test('cancel posts to the work-item cancel endpoint by hash', async () =>
	{
		let tmpClient = fakeUltravisorClient();
		let tmpExec = new UltravisorExecutionClient({ Client: tmpClient });

		await tmpExec.cancel('wi-42');
		let tmpCancel = tmpClient.Calls.find((pCall) => /\/Cancel$/.test(pCall.Path));
		Expect(tmpCancel.Method).to.equal('POST');
		Expect(tmpCancel.Path).to.equal('/Beacon/Work/wi-42/Cancel');
		Expect(tmpCancel.Body).to.have.property('Reason');
	});

	test('enqueue rejects when Ultravisor returns no hash', async () =>
	{
		let tmpExec = new UltravisorExecutionClient({ Client: { request: (pM, pP, pB, fCb) => fCb(null, {}) } });
		let tmpFailed = false;
		try { await tmpExec.enqueue({ UnitKey: 'a' }); } catch (pError) { tmpFailed = true; }
		Expect(tmpFailed).to.equal(true);
	});

	test('drives the scheduler: tick enqueues to Ultravisor, reconcile reports completions back', async () =>
	{
		let tmpEngine = await _engine(libPath.join(_dir, 'oq.sqlite'), 3);
		let tmpClient = fakeUltravisorClient();
		let tmpExec = new UltravisorExecutionClient({ Client: tmpClient });
		tmpEngine.Scheduler.setExecutionClient(tmpExec);
		tmpEngine.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c' ]));

		let tmpGroup = await tmpEngine.activateGroup(_scope, await tmpEngine.planGroup(_scope, { OperationType: 'test.op', Label: 'g', Lane: 'Normal' }));

		let tmpTick = await tmpEngine.Scheduler.tick();
		Expect(tmpTick.Fed).to.equal(3);
		Expect(tmpClient.Calls.filter((pCall) => pCall.Path === '/Beacon/Work/Enqueue').length).to.equal(3);

		let tmpTasks = await tmpEngine.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
		Expect(tmpTasks.map((pTask) => pTask.ExecRef).sort()).to.deep.equal([ 'wi-1', 'wi-2', 'wi-3' ]);

		// reconcile with a resolver that marks everything terminal + successful
		let tmpReported = await tmpExec.reconcile(tmpEngine.Scheduler, () => Promise.resolve({ Terminal: true, Success: true }));
		Expect(tmpReported).to.equal(3);
		Expect(await tmpEngine.Store.countInflightTasks()).to.equal(0);

		let tmpReload = await tmpEngine.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.CompletedUnits).to.equal(3);
	});
});
