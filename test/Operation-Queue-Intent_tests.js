/**
 * retold-operation-queue intent engine: Phase-B tests.
 *
 * Hermetic. Each test boots a standalone engine over a throwaway SQLite file and drives the intent
 * model with a fake enumerator (a fixed array paged by a numeric cursor) and a fake dedup check.
 * Proves: lane normalization + inheritance, lazy cursor-paged expansion, dedup skipping, and
 * whole-group reprioritization. No execution queue, no network.
 */

const libPath = require('path');
const libOS = require('os');
const libFS = require('fs');
const Chai = require('chai');
const Expect = Chai.expect;

const { createStandalone, Lanes } = require('../source/Operation-Queue.js');

function _engine(pFile)
{
	return new Promise((fResolve, fReject) =>
		createStandalone({ SQLiteFilePath: pFile }, (pError, pEngine) => (pError ? fReject(pError) : fResolve(pEngine))));
}

// A fake enumerator: pages a fixed array of unit keys, using the cursor token as a numeric offset.
function pagingEnumerator(pAllKeys)
{
	return (pGroup, pCursorToken, pLimit) =>
	{
		let tmpOffset = pCursorToken ? parseInt(pCursorToken, 10) : 0;
		let tmpSlice = pAllKeys.slice(tmpOffset, tmpOffset + pLimit);
		let tmpNext = tmpOffset + tmpSlice.length;
		return Promise.resolve(
			{
				Units: tmpSlice.map((pKey) => ({ UnitKey: pKey })),
				NextCursorToken: String(tmpNext),
				Exhausted: tmpNext >= pAllKeys.length
			});
	};
}

suite('retold-operation-queue intent', () =>
{
	let _engineInst;
	let _dir;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(async () =>
	{
		_dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-intent-'));
		_engineInst = await _engine(libPath.join(_dir, 'oq.sqlite'));
	});
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	suite('lanes', () =>
	{
		test('normalize is case-insensitive and defaults to Normal', () =>
		{
			Expect(Lanes.normalizeLane('urgent')).to.equal('Urgent');
			Expect(Lanes.normalizeLane('BACKGROUND')).to.equal('Background');
			Expect(Lanes.normalizeLane('nonsense')).to.equal('Normal');
			Expect(Lanes.normalizeLane(undefined)).to.equal('Normal');
		});
		test('priority orders Urgent > Normal > Background', () =>
		{
			Expect(Lanes.laneToPriority('Urgent')).to.be.above(Lanes.laneToPriority('Normal'));
			Expect(Lanes.laneToPriority('Normal')).to.be.above(Lanes.laneToPriority('Background'));
			Expect(Lanes.isLane('Urgent')).to.equal(true);
			Expect(Lanes.isLane('nope')).to.equal(false);
		});
	});

	test('planGroup normalizes the lane, derives priority, and defaults to Planned', async () =>
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'test.op', Label: 'g', Lane: 'background' });
		Expect(tmpGroup.Lane).to.equal('Background');
		Expect(tmpGroup.Priority).to.equal(Lanes.laneToPriority('Background'));
		Expect(tmpGroup.Status).to.equal('Planned');
	});

	test('setLane reprioritizes the whole group', async () =>
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'test.op', Label: 'g', Lane: 'Background' });
		let tmpRaised = await _engineInst.setLane(_scope, tmpGroup, 'Urgent');
		Expect(tmpRaised.Lane).to.equal('Urgent');
		Expect(tmpRaised.Priority).to.equal(Lanes.laneToPriority('Urgent'));
	});

	test('nextBatch expands lazily through the cursor and reports Exhausted', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd', 'e' ]));
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'test.op', Label: 'g', Lane: 'Normal' });

		let tmpB1 = await _engineInst.nextBatch(_scope, tmpGroup, 2);
		Expect(tmpB1.Units.map((pU) => pU.UnitKey)).to.deep.equal([ 'a', 'b' ]);
		Expect(tmpB1.Exhausted).to.equal(false);
		Expect(tmpB1.Units[0]).to.include({ Lane: 'Normal', Priority: Lanes.laneToPriority('Normal'), IDOperationGroup: tmpGroup.IDOperationGroup });

		let tmpB2 = await _engineInst.nextBatch(_scope, tmpGroup, 2);
		Expect(tmpB2.Units.map((pU) => pU.UnitKey)).to.deep.equal([ 'c', 'd' ]);

		let tmpB3 = await _engineInst.nextBatch(_scope, tmpGroup, 2);
		Expect(tmpB3.Units.map((pU) => pU.UnitKey)).to.deep.equal([ 'e' ]);
		Expect(tmpB3.Exhausted).to.equal(true);

		let tmpB4 = await _engineInst.nextBatch(_scope, tmpGroup, 2);
		Expect(tmpB4.Units).to.deep.equal([]);
		Expect(tmpB4.Exhausted).to.equal(true);

		let tmpCursor = await _engineInst.Store.getCursor(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpCursor.YieldedCount).to.equal(5);
	});

	test('nextBatch drops deduped units and counts them as skipped', async () =>
	{
		_engineInst.registerEnumerator('test.op', pagingEnumerator([ 'a', 'b', 'c', 'd' ]));
		_engineInst.registerDedup('test.op', (pGroup, pUnit) => (pUnit.UnitKey === 'b' || pUnit.UnitKey === 'd'));
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'test.op', Label: 'g', Lane: 'Normal' });

		let tmpBatch = await _engineInst.nextBatch(_scope, tmpGroup, 4);
		Expect(tmpBatch.Units.map((pU) => pU.UnitKey)).to.deep.equal([ 'a', 'c' ]);
		Expect(tmpBatch.Skipped).to.equal(2);

		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.SkippedUnits).to.equal(2);
	});

	test('nextBatch rejects when no enumerator is registered for the OperationType', async () =>
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'unregistered.op', Label: 'g', Lane: 'Normal' });
		let tmpFailed = false;
		try { await _engineInst.nextBatch(_scope, tmpGroup, 2); } catch (pError) { tmpFailed = true; }
		Expect(tmpFailed).to.equal(true);
	});
});
