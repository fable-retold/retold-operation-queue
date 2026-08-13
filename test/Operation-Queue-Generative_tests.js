/**
 * retold-operation-queue generative walk: Phase-1 tests (in-place-index enabler).
 *
 * Hermetic. A generative group has no consumer enumerator -- its units come from a frontier that GROWS as
 * units complete. Each test models a tiny directory tree and a fake "enumerate one directory" capability:
 * ticking feeds directory units, and reportOutcome({ Success, Expand: subdirs }) both records what the
 * directory turned up and grows the frontier with its children. Proves: a recursive walk drains to Done and
 * fires the group-done hook; dedup prunes a subtree; pause stops feeding and resume continues; and a restart
 * reconcile re-drives an orphaned unit, dead-lettering it once the attempt cap is hit.
 */

const libPath = require('path');
const libOS = require('os');
const libFS = require('fs');
const Chai = require('chai');
const Expect = Chai.expect;

const { createStandalone } = require('../source/Operation-Queue.js');

// A tiny tree: NodeKey '' is the root. Each node lists its child directories and its files.
const _TREE =
{
	'':    { subdirs: [ 'a', 'b' ],  files: [ 'root.txt' ] },
	'a':   { subdirs: [ 'a/x' ],     files: [ 'a/1.jpg', 'a/2.jpg' ] },
	'a/x': { subdirs: [],            files: [ 'a/x/deep.png' ] },
	'b':   { subdirs: [],            files: [ 'b/only.gif' ] }
};
const _ALL_FILES = [ 'root.txt', 'a/1.jpg', 'a/2.jpg', 'a/x/deep.png', 'b/only.gif' ];

function _engine(pFile, pWindowSize)
{
	return new Promise((fResolve, fReject) =>
		createStandalone({ SQLiteFilePath: pFile, WindowSize: pWindowSize }, (pError, pEngine) => (pError ? fReject(pError) : fResolve(pEngine))));
}

function fakeClient()
{
	let tmpSeq = 0;
	return { enqueue: () => Promise.resolve('x' + (++tmpSeq)), cancel: () => Promise.resolve() };
}

suite('retold-operation-queue generative walk', () =>
{
	let _engineInst;
	let _client;
	let _dir;
	let _scope = { IDUser: 7, IDCustomer: 3 };

	setup(async () =>
	{
		_dir = libFS.mkdtempSync(libPath.join(libOS.tmpdir(), 'roq-gen-'));
		_engineInst = await _engine(libPath.join(_dir, 'oq.sqlite'), 2);
		_client = fakeClient();
		_engineInst.Scheduler.setExecutionClient(_client);
	});
	teardown(() => { try { libFS.rmSync(_dir, { recursive: true, force: true }); } catch (pError) { /* best effort */ } });

	// Drive the walk to quiescence: tick, then complete every fed unit (recording its files + expanding the
	// frontier by its subdirs), until the group leaves Active or a safety cap is hit. Returns the files seen.
	async function _walk(pGroup, pTree)
	{
		let tmpTree = pTree || _TREE;
		let tmpFilesSeen = [];
		for (let tmpRound = 0; tmpRound < 200; tmpRound++)
		{
			await _engineInst.Scheduler.tick();
			let tmpGroup = await _engineInst.Store.readGroup(_scope, pGroup.IDOperationGroup);
			if (!tmpGroup || tmpGroup.Status !== 'Active') { break; }
			let tmpFed = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
			if (tmpFed.length === 0) { break; } // Active but nothing to feed (e.g. paused window) -- stop
			for (let tmpTask of tmpFed)
			{
				let tmpNode = tmpTree[tmpTask.UnitKey] || { subdirs: [], files: [] };
				tmpNode.files.forEach((pFile) => tmpFilesSeen.push(pFile));
				await _engineInst.Scheduler.reportOutcome(tmpTask.ExecRef, { Success: true, Expand: tmpNode.subdirs });
			}
		}
		return tmpFilesSeen;
	}

	test('a generative group walks the whole tree, reaps to Done, and fires the group-done hook', async () =>
	{
		let tmpDoneGroups = [];
		_engineInst.onGroupDone((pScope, pGroup) => tmpDoneGroups.push(pGroup.IDOperationGroup));

		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'index', Label: 'walk', Lane: 'Background', Generative: true });
		tmpGroup = await _engineInst.activateGroup(_scope, tmpGroup);

		let tmpFiles = await _walk(tmpGroup);

		// Every directory was enumerated exactly once (4 dirs), so every file was seen exactly once.
		Expect(tmpFiles.slice().sort()).to.deep.equal(_ALL_FILES.slice().sort());

		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.Status).to.equal('Done');
		Expect(tmpReload.FedUnits).to.equal(4);       // 4 directory units
		Expect(tmpReload.CompletedUnits).to.equal(4);
		Expect(tmpReload.FinishedDate).to.be.a('string');
		Expect(tmpDoneGroups).to.deep.equal([ tmpGroup.IDOperationGroup ]);

		// The frontier fully drained -- nothing pending or walking left behind.
		Expect(await _engineInst.Store.countFrontierActive(_scope, tmpGroup.IDOperationGroup)).to.equal(0);
		// ...and walked nodes are PRUNED, not just flagged, so the table stays bounded to the live wavefront
		// (this is what keeps a million-directory walk's exhaustion checks cheap).
		let tmpAllFrontier = await _engineInst.Store._reads('OperationFrontier', (pQuery) => pQuery.addFilter('IDOperationGroup', tmpGroup.IDOperationGroup));
		Expect(tmpAllFrontier.length).to.equal(0);
	});

	test('a dedup check prunes a whole subtree (an unchanged directory is skipped, its children never discovered)', async () =>
	{
		// Skip directory 'b' (as an unchanged-mtime directory would be on a re-index).
		_engineInst.registerDedup('index', (pGroup, pUnit) => (pUnit.UnitKey === 'b'));

		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'index', Label: 'walk', Lane: 'Background', Generative: true });
		tmpGroup = await _engineInst.activateGroup(_scope, tmpGroup);

		let tmpFiles = await _walk(tmpGroup);

		Expect(tmpFiles).to.not.include('b/only.gif');       // 'b' was skipped, so its file is never seen
		Expect(tmpFiles.slice().sort()).to.deep.equal([ 'a/1.jpg', 'a/2.jpg', 'a/x/deep.png', 'root.txt' ].sort());
		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.Status).to.equal('Done');
		Expect(tmpReload.SkippedUnits).to.equal(1);          // 'b'
		Expect(tmpReload.FedUnits).to.equal(3);              // '', 'a', 'a/x'
	});

	test('pauseGroup stops feeding; the in-flight window drains; resumeGroup finishes the walk', async () =>
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'index', Label: 'walk', Lane: 'Background', Generative: true });
		tmpGroup = await _engineInst.activateGroup(_scope, tmpGroup);

		// One tick feeds the root directory.
		await _engineInst.Scheduler.tick();
		let tmpInflight = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
		Expect(tmpInflight.length).to.equal(1);

		// Pause: the next tick feeds NOTHING even though the frontier has work.
		await _engineInst.pauseGroup(_scope, tmpGroup);
		let tmpPausedTick = await _engineInst.Scheduler.tick();
		Expect(tmpPausedTick.Fed).to.equal(0);
		Expect((await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Paused');

		// Drain the in-flight unit (it discovers 'a' and 'b'); still paused, so nothing new is fed.
		let tmpRoot = tmpInflight[0];
		await _engineInst.Scheduler.reportOutcome(tmpRoot.ExecRef, { Success: true, Expand: _TREE[''].subdirs });
		let tmpStillPaused = await _engineInst.Scheduler.tick();
		Expect(tmpStillPaused.Fed).to.equal(0);
		Expect((await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Paused');

		// Resume and finish. The remaining files (everything except root.txt, already seen) come through.
		let tmpResumed = await _engineInst.resumeGroup(_scope, tmpGroup);
		let tmpRest = await _walk(tmpResumed);
		Expect(tmpRest.slice().sort()).to.deep.equal([ 'a/1.jpg', 'a/2.jpg', 'a/x/deep.png', 'b/only.gif' ].sort());
		Expect((await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup)).Status).to.equal('Done');
	});

	test('reconcileInflight re-drives an orphaned unit, and dead-letters it once the attempt cap is hit', async () =>
	{
		let tmpGroup = await _engineInst.planGroup(_scope, { OperationType: 'index', Label: 'walk', Lane: 'Background', Generative: true });
		tmpGroup = await _engineInst.activateGroup(_scope, tmpGroup);

		// Feed the root, then "crash": leave it Fed, never reported.
		await _engineInst.Scheduler.tick();
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(1);

		// First reconcile (cap 2): under the cap -> re-drive. The task retires and the frontier node is
		// pending again, so it re-feeds and the window frees.
		let tmpFirst = await _engineInst.Scheduler.reconcileInflight(2);
		Expect(tmpFirst).to.deep.equal({ Requeued: 1, DeadLettered: 0 });
		Expect(await _engineInst.Store.countInflightTasks()).to.equal(0);

		// The attempt count persists on the frontier NODE (a re-fed task starts fresh), so it accrues.
		let tmpNode = await _engineInst.Store.getFrontierNode(_scope, tmpGroup.IDOperationGroup, '');
		Expect(tmpNode.Attempt).to.equal(1);

		// It re-feeds on the next tick.
		await _engineInst.Scheduler.tick();
		let tmpRefed = await _engineInst.Store.readTasks(_scope, (pQuery) => pQuery.addFilter('Status', 'Fed'));
		Expect(tmpRefed.length).to.equal(1);
		Expect(tmpRefed[0].UnitKey).to.equal('');

		// Crash again and reconcile past the cap -> dead-letter: the node is done, the task Failed, counted.
		let tmpSecond = await _engineInst.Scheduler.reconcileInflight(1);
		Expect(tmpSecond).to.deep.equal({ Requeued: 0, DeadLettered: 1 });
		let tmpReload = await _engineInst.Store.readGroup(_scope, tmpGroup.IDOperationGroup);
		Expect(tmpReload.FailedUnits).to.equal(1);
		Expect(await _engineInst.Store.countFrontierActive(_scope, tmpGroup.IDOperationGroup)).to.equal(0);
	});
});
