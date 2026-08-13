/**
 * Operation Queue Scheduler: the pace governor.
 *
 * It meters a BOUNDED WINDOW of work into an injected execution client (Ultravisor in production, a fake
 * in tests) and refills the window as work completes. The execution queue only ever holds "what is
 * running now"; the vast backlog stays in the intent layer as groups + cursors. Because only a window
 * of tasks is ever materialized (Fed/Running) or kept (Failed), reprioritize and cancel are O(window).
 *
 * Injected execution client contract:
 *   enqueue(pUnit) -> execRef | Promise<execRef>   (hand a unit to the execution queue)
 *   cancel(pExecRef) -> void | Promise             (optional; cancel an in-flight item)
 *
 * The consumer (or the client's telemetry adapter) calls reportOutcome(execRef, {Success, Error}) when
 * a work item finishes; that frees its window slot, so the next tick() refills. Backpressure is
 * automatic: if nothing completes, the window stays full and tick() feeds nothing.
 */

const libFableServiceProviderBase = require('fable-serviceproviderbase');

class OperationQueueScheduler extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
		this.serviceType = 'OperationQueueScheduler';
		this.Intent = null;
		this.ExecutionClient = null;
		this.WindowSize = (this.options && this.options.WindowSize) || 8;
	}

	setIntent(pIntent) { this.Intent = pIntent; return this; }
	setExecutionClient(pClient) { this.ExecutionClient = pClient; return this; }
	get Store() { return this.Intent && this.Intent.Store; }

	// One metering pass: fill the free window with the highest-priority pending work.
	async tick()
	{
		if (!this.ExecutionClient) { throw new Error('No execution client set; call setExecutionClient() first.'); }

		let tmpOccupancy = await this.Store.countInflightTasks();
		let tmpRoom = this.WindowSize - tmpOccupancy;
		if (tmpRoom <= 0) { return { Fed: 0, Occupancy: tmpOccupancy, RoomLeft: 0 }; }

		// Highest lane/priority first. Groups are coarse (far fewer than tasks), so a JS sort is fine.
		let tmpGroups = (await this.Store.readGroupsByStatus('Active')).slice()
			.sort((pA, pB) => (pB.Priority || 0) - (pA.Priority || 0));

		let tmpFed = 0;
		for (let tmpGroup of tmpGroups)
		{
			if (tmpRoom <= 0) { break; }
			let tmpScope = { IDUser: tmpGroup.OwnerIDUser, IDCustomer: tmpGroup.IDCustomer };
			// Pull at most `room` survivors so nothing is enumerated past the cursor without being fed.
			let tmpBatch = await this.Intent.nextBatch(tmpScope, tmpGroup, tmpRoom);
			for (let tmpUnit of tmpBatch.Units)
			{
				if (tmpRoom <= 0) { break; }
				let tmpExecRef = await this.ExecutionClient.enqueue(tmpUnit);
				await this.Store.createTask(tmpScope,
					{
						IDOperationGroup: tmpGroup.IDOperationGroup,
						UnitKey: tmpUnit.UnitKey,
						Lane: tmpUnit.Lane,
						Priority: tmpUnit.Priority,
						Status: 'Fed',
						ExecRef: String(tmpExecRef)
					});
				tmpGroup.FedUnits = (tmpGroup.FedUnits || 0) + 1;
				await this.Store.updateGroup(tmpScope, { IDOperationGroup: tmpGroup.IDOperationGroup, FedUnits: tmpGroup.FedUnits });
				tmpFed++;
				tmpRoom--;
			}
		}

		// Reap any Active group that has fully drained. This catches the case a completion cannot -- a group
		// that fed nothing (an empty enumeration, or one already fully done) never produces an outcome to
		// trigger reportOutcome's reap. Progressing groups fall through cheaply on the in-flight check.
		for (let tmpGroup of tmpGroups)
		{
			await this._reapIfDone({ IDUser: tmpGroup.OwnerIDUser, IDCustomer: tmpGroup.IDCustomer }, tmpGroup);
		}
		return { Fed: tmpFed, Occupancy: tmpOccupancy + tmpFed, RoomLeft: tmpRoom };
	}

	// A work item finished. Mark its task Done or Failed and bump the group's counts; the freed slot is
	// picked up by the next tick(). Unknown refs (already reconciled) are a no-op.
	async reportOutcome(pExecRef, pOutcome)
	{
		let tmpTask = await this.Store.readTaskByExecRef(String(pExecRef));
		if (!tmpTask) { return null; }
		let tmpScope = { IDUser: tmpTask.OwnerIDUser, IDCustomer: tmpTask.IDCustomer };
		let tmpSuccess = !!(pOutcome && pOutcome.Success);

		if (tmpSuccess)
		{
			await this.Store.updateTask(tmpScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Done' });
			await this._bumpGroupCount(tmpScope, tmpTask.IDOperationGroup, 'CompletedUnits');
		}
		else
		{
			await this.Store.updateTask(tmpScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Failed', LastError: (pOutcome && pOutcome.Error) || 'failed' });
			await this._bumpGroupCount(tmpScope, tmpTask.IDOperationGroup, 'FailedUnits');
		}

		// For a generative group, the completed unit's frontier node closes out: on success with discovered
		// children it EXPANDS the frontier (the recursion), otherwise it just marks the node done. Then check
		// whether the whole group has drained and, if so, reap it to Done (the module owns that transition).
		let tmpGroup = await this.Store.readGroup(tmpScope, tmpTask.IDOperationGroup);
		if (tmpGroup && tmpGroup.Generative)
		{
			if (tmpSuccess && pOutcome && Array.isArray(pOutcome.Expand) && pOutcome.Expand.length > 0)
			{
				await this.Intent.expandFrontier(tmpScope, tmpGroup, tmpTask.UnitKey, pOutcome.Expand);
			}
			else
			{
				await this.Intent.completeFrontierNode(tmpScope, tmpGroup, tmpTask.UnitKey);
			}
		}
		await this._reapIfDone(tmpScope, tmpGroup || { IDOperationGroup: tmpTask.IDOperationGroup });
		return tmpTask.IDOperationTask;
	}

	// Reap a group to Done when it has drained: its expansion is exhausted (cursor Exhausted, or the
	// generative frontier is empty) AND none of its tasks are still in flight. Fires the intent's group-done
	// hook so consumers can react. This transition lives in the module so every consumer gets the same Done.
	async _reapIfDone(pScope, pGroup)
	{
		let tmpGroup = await this.Store.readGroup(pScope, pGroup.IDOperationGroup);
		if (!tmpGroup || tmpGroup.Status !== 'Active') { return false; }
		// Cheapest disqualifier first: a group with any in-flight task cannot be done. Counting the handful of
		// Fed/Running rows short-circuits every progressing group here, so the heavier frontier-exhaustion read
		// only runs once a walk has actually drained -- not on every tick while it is climbing.
		let tmpInflight = await this.Store.countInflightForGroup(pScope, tmpGroup.IDOperationGroup);
		if (tmpInflight > 0) { return false; }
		let tmpExhausted = await this.Intent.isExpansionExhausted(pScope, tmpGroup);
		if (!tmpExhausted) { return false; }
		await this.Store.updateGroup(pScope, { IDOperationGroup: tmpGroup.IDOperationGroup, Status: 'Done', FinishedDate: new Date().toISOString() });
		let tmpDone = await this.Store.readGroup(pScope, tmpGroup.IDOperationGroup);
		this.Intent._fireGroupDone(pScope, tmpDone || tmpGroup);
		return true;
	}

	/**
	 * Restart reconcile: re-drive tasks a crash left in-flight (Fed/Running). Idempotent, and guarded by a
	 * per-task attempt cap so a node that keeps crashing dead-letters to Failed instead of looping forever.
	 * Generative tasks reset their frontier node back to pending (re-claimed next tick); a classic task,
	 * whose cursor has already moved past it, is retired Failed so its slot frees and the consumer re-drives
	 * it on demand. Call once on boot, before pumping. Returns { Requeued, DeadLettered }.
	 */
	async reconcileInflight(pAttemptCap)
	{
		let tmpCap = parseInt(pAttemptCap, 10) || 5;
		let tmpOrphans = await this.Store.readInflightTasks();
		let tmpRequeued = 0;
		let tmpDead = 0;
		for (let tmpTask of tmpOrphans)
		{
			let tmpScope = { IDUser: tmpTask.OwnerIDUser, IDCustomer: tmpTask.IDCustomer };
			let tmpGroup = await this.Store.readGroup(tmpScope, tmpTask.IDOperationGroup);
			let tmpGenerative = !!(tmpGroup && tmpGroup.Generative);
			let tmpNode = tmpGenerative ? await this.Store.getFrontierNode(tmpScope, tmpTask.IDOperationGroup, tmpTask.UnitKey) : null;
			// The attempt count PERSISTS on the frontier node across re-feeds (a re-fed task starts fresh),
			// so the node is the source of truth for a generative walk; a classic task uses its own Attempt.
			let tmpPrior = tmpNode ? (parseInt(tmpNode.Attempt, 10) || 0) : (parseInt(tmpTask.Attempt, 10) || 0);
			let tmpAttempt = tmpPrior + 1;
			if (tmpGenerative && tmpAttempt <= tmpCap)
			{
				if (tmpNode) { await this.Store.updateFrontier(tmpScope, { IDOperationFrontier: tmpNode.IDOperationFrontier, State: 'pending', Attempt: tmpAttempt }); }
				await this.Store.updateTask(tmpScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Reconciled', Attempt: tmpAttempt });
				tmpRequeued++;
			}
			else
			{
				let tmpReason = tmpGenerative ? ('dead-lettered after ' + tmpCap + ' attempts') : 'orphaned by restart';
				if (tmpNode) { await this.Store.deleteFrontier(tmpNode.IDOperationFrontier); }
				await this.Store.updateTask(tmpScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Failed', Attempt: tmpAttempt, LastError: tmpReason });
				await this._bumpGroupCount(tmpScope, tmpTask.IDOperationGroup, 'FailedUnits');
				tmpDead++;
			}
		}
		return { Requeued: tmpRequeued, DeadLettered: tmpDead };
	}

	// Cancel a whole group: mark it Canceled and cancel its in-flight tasks through the execution client.
	// O(window): only in-flight tasks are materialized, so there is never a huge set to walk.
	async cancelGroup(pScope, pGroup)
	{
		await this.Store.updateGroup(pScope, { IDOperationGroup: pGroup.IDOperationGroup, Status: 'Canceled' });
		let tmpInflight = await this.Store.readInflightTasksForGroup(pScope, pGroup.IDOperationGroup);
		for (let tmpTask of tmpInflight)
		{
			if (this.ExecutionClient && typeof this.ExecutionClient.cancel === 'function' && tmpTask.ExecRef)
			{
				try { await this.ExecutionClient.cancel(tmpTask.ExecRef); }
				catch (pCancelError) { /* best effort -- still mark the task canceled below */ }
			}
			await this.Store.updateTask(pScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Canceled' });
		}
		return tmpInflight.length;
	}

	async _bumpGroupCount(pScope, pIDOperationGroup, pField)
	{
		let tmpGroup = await this.Store.readGroup(pScope, pIDOperationGroup);
		if (!tmpGroup) { return; }
		let tmpUpdate = { IDOperationGroup: pIDOperationGroup };
		tmpUpdate[pField] = (tmpGroup[pField] || 0) + 1;
		await this.Store.updateGroup(pScope, tmpUpdate);
	}
}

module.exports = OperationQueueScheduler;
