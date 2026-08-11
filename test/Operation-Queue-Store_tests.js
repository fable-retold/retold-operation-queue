/**
 * retold-operation-queue store: Phase-A foundation tests.
 *
 * Hermetic -- each test boots a standalone instance over a throwaway SQLite file, exercises the three
 * entities (group / task / cursor), and proves owner-scoping is stamped on create and enforced on read.
 * No Ultravisor, no network.
 */

const libPath = require('path');
const libOS = require('os');
const libFS = require('fs');
const Chai = require('chai');
const Expect = Chai.expect;

const { createStandalone } = require('../source/Operation-Queue.js');

function _standalone(pFile)
{
	return new Promise((fResolve, fReject) =>
		createStandalone({ SQLiteFilePath: pFile }, (pError, pStore) => (pError ? fReject(pError) : fResolve(pStore))));
}

suite('retold-operation-queue store', () =>
{
	let _store;
	let _dir;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(async () =>
	{
		_dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-'));
		_store = await _standalone(libPath.join(_dir, 'oq.sqlite'));
	});
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	test('connect builds a DAL per entity', () =>
	{
		Expect(_store.Connected).to.equal(true);
		Expect(Object.keys(_store.DAL).sort()).to.deep.equal([ 'OperationCursor', 'OperationGroup', 'OperationTask' ]);
	});

	test('a group round-trips and is stamped with the owner scope', async () =>
	{
		let tmpGroup = await _store.createGroup(_scope, {
			ConsumerKey: 'sluice', OperationType: 'rendition.thumbnail', Label: 'Thumbnail /Photos/2019',
			SourceRef: '2/Photos/2019', Lane: 'Background', Priority: 10, Status: 'Planned', TotalUnits: 240000 });
		Expect(tmpGroup.IDOperationGroup).to.be.a('number');
		Expect(tmpGroup.OwnerIDUser).to.equal(7);
		Expect(tmpGroup.IDCustomer).to.equal(3);
		Expect(tmpGroup.CreatingIDUser).to.equal(7);

		let tmpRead = await _store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpRead.Label).to.equal('Thumbnail /Photos/2019');
		Expect(tmpRead.TotalUnits).to.equal(240000);
		Expect(tmpRead.Lane).to.equal('Background');
	});

	test('another owner cannot read the group', async () =>
	{
		let tmpGroup = await _store.createGroup(_scope, { Label: 'mine', Status: 'Planned' });
		let tmpOther = await _store.readGroup({ IDUser: 99, IDCustomer: 3 }, tmpGroup.IDOperationGroup);
		Expect(tmpOther).to.equal(null);
	});

	test('tasks attach to a group and update status', async () =>
	{
		let tmpGroup = await _store.createGroup(_scope, { Label: 'g', Status: 'Active' });
		let tmpTask = await _store.createTask(_scope, {
			IDOperationGroup: tmpGroup.IDOperationGroup, UnitKey: '2/Photos/2019/a.jpg',
			Lane: 'Urgent', Priority: 100, Status: 'Fed', ExecRef: 'wi-abc' });
		Expect(tmpTask.IDOperationTask).to.be.a('number');

		await _store.updateTask(_scope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Done' });
		let tmpTasks = await _store.readTasks(_scope, (pQuery) => pQuery.addFilter('IDOperationGroup', tmpGroup.IDOperationGroup));
		Expect(tmpTasks.length).to.equal(1);
		Expect(tmpTasks[0].Status).to.equal('Done');
		Expect(tmpTasks[0].ExecRef).to.equal('wi-abc');
	});

	test('the cursor upserts (one per group) and advances', async () =>
	{
		let tmpGroup = await _store.createGroup(_scope, { Label: 'g', Status: 'Active' });
		await _store.upsertCursor(_scope, { IDOperationGroup: tmpGroup.IDOperationGroup, CursorToken: 'path=a.jpg', YieldedCount: 100, Exhausted: 0 });
		await _store.upsertCursor(_scope, { IDOperationGroup: tmpGroup.IDOperationGroup, CursorToken: 'path=z.jpg', YieldedCount: 200, Exhausted: 1 });

		let tmpCursor = await _store.getCursor(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpCursor.CursorToken).to.equal('path=z.jpg');
		Expect(tmpCursor.YieldedCount).to.equal(200);
		Expect(!!tmpCursor.Exhausted).to.equal(true);

		let tmpAllCursors = await _store._reads('OperationCursor', (pQuery) => pQuery.addFilter('IDOperationGroup', tmpGroup.IDOperationGroup));
		Expect(tmpAllCursors.length).to.equal(1);
	});
});
