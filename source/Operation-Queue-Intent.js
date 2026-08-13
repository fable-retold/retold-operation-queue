/**
 * Operation Queue Intent engine: the durable plan on top of the store.
 *
 * This is where "everything we intend to do" lives. A group is planned with a lane; its leaf units are
 * expanded LAZILY through a consumer-supplied enumerator and a resumable cursor, so a group covering
 * millions of units never materializes millions of rows. A consumer-supplied dedup check drops units
 * that are already done (a cache hit) before they are ever handed on. Lanes are inherited by the units
 * and can be raised or lowered for a whole group in one move (reprioritize).
 *
 * The consumer supplies three things, keyed by OperationType:
 *   - an enumerator: (pGroup, pCursorToken, pLimit) -> {Units:[{UnitKey, Params?}], NextCursorToken, Exhausted}
 *   - (optional) a dedup check: (pGroup, pUnit) -> boolean|Promise<boolean>   (true = already done, skip)
 *
 * nextBatch() returns the survivors ready to be fed to an execution queue (Phase C metering); it does
 * NOT persist task rows -- the pending backlog stays virtual until a unit is actually fed.
 */

const libFableServiceProviderBase = require('fable-serviceproviderbase');
const libLanes = require('./Operation-Queue-Lanes.js');

class OperationQueueIntent extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
		this.serviceType = 'OperationQueueIntent';
		this.Store = null;
		this.Lanes = libLanes;
		this._Enumerators = {};
		this._Dedupers = {};
		this._GroupDoneHooks = [];
		this._DefaultBatchLimit = (this.options && this.options.BatchLimit) || 100;
	}

	setStore(pStore) { this.Store = pStore; return this; }

	// Register how to list a group's units for an OperationType (the lazy expansion source).
	registerEnumerator(pOperationType, fEnumerator) { this._Enumerators[pOperationType] = fEnumerator; return this; }

	// Register an optional dedup check for an OperationType (true = already done, so skip the unit).
	registerDedup(pOperationType, fDedup) { this._Dedupers[pOperationType] = fDedup; return this; }

	// Create a group in Planned state with a normalized lane and its derived numeric priority. A GENERATIVE
	// group (Generative: true, optional Seed: [{ NodeKey, Depth }]) seeds the frontier with its starting
	// wavefront (default a single root node) so its units are discovered by walking rather than enumerated
	// from a known list; its leaf tasks expand as they complete (see expandFrontier / nextBatch).
	planGroup(pScope, pGroupSpec)
	{
		let tmpSelf = this;
		let tmpSpec = Object.assign({}, pGroupSpec);
		tmpSpec.Lane = libLanes.normalizeLane(tmpSpec.Lane);
		tmpSpec.Priority = libLanes.laneToPriority(tmpSpec.Lane);
		if (!tmpSpec.Status) { tmpSpec.Status = 'Planned'; }
		let tmpGenerative = !!tmpSpec.Generative;
		let tmpSeed = tmpSpec.Seed;
		delete tmpSpec.Seed;                          // Seed is a plan input, not a group column
		tmpSpec.Generative = tmpGenerative ? 1 : 0;
		return this.Store.createGroup(pScope, tmpSpec).then((pGroup) =>
		{
			if (!tmpGenerative) { return pGroup; }
			let tmpSeedNodes = (Array.isArray(tmpSeed) && tmpSeed.length ? tmpSeed : [ { NodeKey: '', Depth: 0 } ])
				.map((pNode) => ({ IDOperationGroup: pGroup.IDOperationGroup, NodeKey: pNode.NodeKey || '', Depth: pNode.Depth || 0 }));
			return tmpSelf.Store.createFrontierNodes(pScope, tmpSeedNodes).then(() => pGroup);
		});
	}

	// Raise or lower a whole group's lane (reprioritize). Returns the reloaded group.
	setLane(pScope, pGroup, pLane)
	{
		let tmpSelf = this;
		let tmpLane = libLanes.normalizeLane(pLane);
		return this.Store.updateGroup(pScope,
			{
				IDOperationGroup: pGroup.IDOperationGroup,
				Lane: tmpLane,
				Priority: libLanes.laneToPriority(tmpLane)
			})
			.then(() => tmpSelf.Store.readGroup(pScope, pGroup.IDOperationGroup));
	}

	// Move a group from Planned to Active so the scheduler will feed it. Returns the reloaded group.
	activateGroup(pScope, pGroup)
	{
		let tmpSelf = this;
		return this.Store.updateGroup(pScope, { IDOperationGroup: pGroup.IDOperationGroup, Status: 'Active' })
			.then(() => tmpSelf.Store.readGroup(pScope, pGroup.IDOperationGroup));
	}

	// Pause a group: flip Active -> Paused. The scheduler meters only Active groups, so a paused group stops
	// being fed immediately; the in-flight window is left to DRAIN (not canceled), and the cursor/frontier
	// persist, so resumeGroup picks up exactly where the wavefront was. Returns the reloaded group.
	pauseGroup(pScope, pGroup)
	{
		let tmpSelf = this;
		return this.Store.updateGroup(pScope, { IDOperationGroup: pGroup.IDOperationGroup, Status: 'Paused' })
			.then(() => tmpSelf.Store.readGroup(pScope, pGroup.IDOperationGroup));
	}

	// Resume a paused group: flip Paused -> Active so the next tick feeds it again. Returns the reloaded group.
	resumeGroup(pScope, pGroup)
	{
		let tmpSelf = this;
		return this.Store.updateGroup(pScope, { IDOperationGroup: pGroup.IDOperationGroup, Status: 'Active' })
			.then(() => tmpSelf.Store.readGroup(pScope, pGroup.IDOperationGroup));
	}

	// Register a callback fired when any group transitions to Done. The module owns the group lifecycle
	// (Planned/Active/Paused/Done/Canceled) and the Done transition; a consumer subscribes here to react at
	// completion (a final activity line, a mirror flip) without owning the transition. A hook never throws.
	onGroupDone(fCallback) { if (typeof fCallback === 'function') { this._GroupDoneHooks.push(fCallback); } return this; }

	_fireGroupDone(pScope, pGroup)
	{
		for (let tmpIndex = 0; tmpIndex < this._GroupDoneHooks.length; tmpIndex++)
		{
			try { this._GroupDoneHooks[tmpIndex](pScope, pGroup); }
			catch (pError) { /* a done hook must never break the queue */ }
		}
	}

	// Whether a group has nothing left to expand: a generative group when its frontier has no active nodes,
	// a classic (enumerator) group when its cursor is Exhausted. The scheduler pairs this with a zero
	// in-flight check to decide a group is Done.
	isExpansionExhausted(pScope, pGroup)
	{
		if (pGroup.Generative)
		{
			return this.Store.countFrontierActive(pScope, pGroup.IDOperationGroup).then((pCount) => pCount === 0);
		}
		return this.Store.getCursor(pScope, pGroup.IDOperationGroup).then((pCursor) => !!(pCursor && pCursor.Exhausted));
	}

	// A completed generative unit expanded into child nodes: mark its node done and add the children pending,
	// so the walk grows by exactly what the unit discovered. Children may be { NodeKey, Depth } or bare
	// strings; Depth defaults to the parent's + 1.
	expandFrontier(pScope, pGroup, pParentNodeKey, pChildren)
	{
		let tmpSelf = this;
		return this.Store.getFrontierNode(pScope, pGroup.IDOperationGroup, pParentNodeKey).then((pParent) =>
		{
			let tmpMarkParent = pParent
				? tmpSelf.Store.deleteFrontier(pParent.IDOperationFrontier)
				: Promise.resolve();
			return tmpMarkParent.then(() =>
			{
				let tmpParentDepth = (pParent && typeof pParent.Depth === 'number') ? pParent.Depth : 0;
				let tmpChildren = (pChildren || []).map((pChild) =>
				{
					let tmpKey = (pChild && typeof pChild === 'object') ? pChild.NodeKey : pChild;
					let tmpDepth = (pChild && typeof pChild === 'object' && typeof pChild.Depth === 'number') ? pChild.Depth : (tmpParentDepth + 1);
					return { IDOperationGroup: pGroup.IDOperationGroup, NodeKey: String(tmpKey), Depth: tmpDepth };
				});
				return tmpChildren.length ? tmpSelf.Store.createFrontierNodes(pScope, tmpChildren) : Promise.resolve([]);
			});
		});
	}

	// Mark a generative node done with no expansion (a leaf directory, or one whose enumeration failed).
	completeFrontierNode(pScope, pGroup, pNodeKey)
	{
		let tmpSelf = this;
		return this.Store.getFrontierNode(pScope, pGroup.IDOperationGroup, pNodeKey).then((pNode) =>
			(pNode ? tmpSelf.Store.deleteFrontier(pNode.IDOperationFrontier) : null));
	}

	/**
	 * Pull the next batch of units for a group: read the saved cursor, call the consumer's enumerator
	 * from there, drop units the dedup check reports as already done, advance + persist the cursor, and
	 * bump the group's SkippedUnits. Returns { Units, Skipped, Exhausted } where each surviving unit is
	 * stamped with the group's inherited Lane, Priority, and IDOperationGroup, ready for Phase C to feed.
	 */
	nextBatch(pScope, pGroup, pLimit)
	{
		if (pGroup.Generative) { return this._nextBatchGenerative(pScope, pGroup, pLimit || this._DefaultBatchLimit); }
		let tmpSelf = this;
		let tmpLimit = pLimit || this._DefaultBatchLimit;
		let tmpEnumerator = this._Enumerators[pGroup.OperationType];
		if (!tmpEnumerator)
		{
			return Promise.reject(new Error('No enumerator registered for OperationType [' + pGroup.OperationType + '].'));
		}
		let tmpDedup = this._Dedupers[pGroup.OperationType];

		return this.Store.getCursor(pScope, pGroup.IDOperationGroup).then((pCursor) =>
		{
			if (pCursor && pCursor.Exhausted) { return { Units: [], Skipped: 0, Exhausted: true }; }
			let tmpToken = pCursor ? pCursor.CursorToken : '';

			return Promise.resolve(tmpEnumerator(pGroup, tmpToken, tmpLimit)).then((pResult) =>
			{
				let tmpRaw = (pResult && pResult.Units) || [];
				return tmpSelf._filterDeduped(pGroup, tmpRaw, tmpDedup).then((pSurvivors) =>
				{
					let tmpSkipped = tmpRaw.length - pSurvivors.length;
					let tmpExhausted = !!(pResult && pResult.Exhausted);
					let tmpUnits = pSurvivors.map((pUnit) => Object.assign({}, pUnit,
						{
							IDOperationGroup: pGroup.IDOperationGroup,
							Lane: pGroup.Lane,
							Priority: pGroup.Priority
						}));

					return tmpSelf.Store.upsertCursor(pScope,
						{
							IDOperationGroup: pGroup.IDOperationGroup,
							CursorToken: (pResult && typeof pResult.NextCursorToken !== 'undefined') ? pResult.NextCursorToken : tmpToken,
							YieldedCount: ((pCursor && pCursor.YieldedCount) || 0) + tmpRaw.length,
							Exhausted: tmpExhausted ? 1 : 0
						})
						.then(() => (tmpSkipped > 0
							? tmpSelf.Store.updateGroup(pScope, { IDOperationGroup: pGroup.IDOperationGroup, SkippedUnits: ((pGroup.SkippedUnits || 0) + tmpSkipped) })
							: null))
						.then(() => ({ Units: tmpUnits, Skipped: tmpSkipped, Exhausted: tmpExhausted }));
				});
			});
		});
	}

	/**
	 * The generative-group batch: pull the next pending frontier nodes, drop the ones dedup reports as
	 * already done (marking them done + bumping SkippedUnits), CLAIM the survivors as 'walking' so a later
	 * tick will not hand them out again, and return them as units. The frontier is the resumable position --
	 * no offset cursor -- and it grows as units complete (see expandFrontier), so the walk is recursive.
	 */
	_nextBatchGenerative(pScope, pGroup, pLimit)
	{
		let tmpSelf = this;
		let tmpDedup = this._Dedupers[pGroup.OperationType];
		return this.Store.readPendingFrontier(pScope, pGroup.IDOperationGroup, pLimit).then((pNodes) =>
		{
			let tmpCandidates = pNodes.map((pNode) => ({ UnitKey: pNode.NodeKey, Depth: pNode.Depth, _Node: pNode }));
			return tmpSelf._filterDeduped(pGroup, tmpCandidates, tmpDedup).then((pSurvivors) =>
			{
				let tmpDeduped = tmpCandidates.filter((pCandidate) => pSurvivors.indexOf(pCandidate) === -1);
				return Promise.all(
					tmpDeduped.map((pCandidate) => tmpSelf.Store.updateFrontier(pScope, { IDOperationFrontier: pCandidate._Node.IDOperationFrontier, State: 'done' }))
						.concat(pSurvivors.map((pCandidate) => tmpSelf.Store.updateFrontier(pScope, { IDOperationFrontier: pCandidate._Node.IDOperationFrontier, State: 'walking' }))))
					.then(() => (tmpDeduped.length > 0
						? tmpSelf.Store.updateGroup(pScope, { IDOperationGroup: pGroup.IDOperationGroup, SkippedUnits: ((pGroup.SkippedUnits || 0) + tmpDeduped.length) })
						: null))
					.then(() =>
					{
						let tmpUnits = pSurvivors.map((pCandidate) => (
							{
								UnitKey: pCandidate.UnitKey,
								Depth: pCandidate.Depth,
								IDOperationGroup: pGroup.IDOperationGroup,
								Lane: pGroup.Lane,
								Priority: pGroup.Priority
							}));
						// Exhausted for THIS pull = there was nothing pending to pull. The group is not Done
						// until the frontier's walking nodes also drain (the scheduler checks that).
						return { Units: tmpUnits, Skipped: tmpDeduped.length, Exhausted: (pNodes.length === 0) };
					});
			});
		});
	}

	// Run each unit through the dedup check (if any) concurrently; keep the ones NOT already done.
	_filterDeduped(pGroup, pUnits, fDedup)
	{
		if (!fDedup || pUnits.length === 0) { return Promise.resolve(pUnits); }
		return Promise.all(pUnits.map((pUnit) =>
			Promise.resolve(fDedup(pGroup, pUnit)).then((pDone) => (pDone ? null : pUnit))))
			.then((pResults) => pResults.filter((pUnit) => pUnit !== null));
	}
}

module.exports = OperationQueueIntent;
