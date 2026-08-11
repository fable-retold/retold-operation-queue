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
		this._DefaultBatchLimit = (this.options && this.options.BatchLimit) || 100;
	}

	setStore(pStore) { this.Store = pStore; return this; }

	// Register how to list a group's units for an OperationType (the lazy expansion source).
	registerEnumerator(pOperationType, fEnumerator) { this._Enumerators[pOperationType] = fEnumerator; return this; }

	// Register an optional dedup check for an OperationType (true = already done, so skip the unit).
	registerDedup(pOperationType, fDedup) { this._Dedupers[pOperationType] = fDedup; return this; }

	// Create a group in Planned state with a normalized lane and its derived numeric priority.
	planGroup(pScope, pGroupSpec)
	{
		let tmpSpec = Object.assign({}, pGroupSpec);
		tmpSpec.Lane = libLanes.normalizeLane(tmpSpec.Lane);
		tmpSpec.Priority = libLanes.laneToPriority(tmpSpec.Lane);
		if (!tmpSpec.Status) { tmpSpec.Status = 'Planned'; }
		return this.Store.createGroup(pScope, tmpSpec);
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

	/**
	 * Pull the next batch of units for a group: read the saved cursor, call the consumer's enumerator
	 * from there, drop units the dedup check reports as already done, advance + persist the cursor, and
	 * bump the group's SkippedUnits. Returns { Units, Skipped, Exhausted } where each surviving unit is
	 * stamped with the group's inherited Lane, Priority, and IDOperationGroup, ready for Phase C to feed.
	 */
	nextBatch(pScope, pGroup, pLimit)
	{
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
