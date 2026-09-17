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
 * An execRef must be unique across restarts, not just within one process: finished task rows are kept, and a
 * completion resolves only a live (Fed/Running) task by its execRef. A unit re-driven after a restart arrives
 * with pUnit.Attempt > 0, so a consumer whose work has side effects can check whether the interrupted attempt
 * already got that far (see reconcileInflight).
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
		// '<IDOperationGroup>:<phase>' -> the last tick error logged for it, so a group that keeps failing logs once.
		this._GroupErrors = new Map();
	}

	setIntent(pIntent) { this.Intent = pIntent; return this; }
	setExecutionClient(pClient) { this.ExecutionClient = pClient; return this; }
	get Store() { return this.Intent && this.Intent.Store; }

	// One metering pass: fill the free window with the highest-priority pending work.
	/**
	 * Hand one unit to the execution client, durably.
	 *
	 * THE ROW IS WRITTEN BEFORE THE HAND-OFF, and that ordering is the whole point. It used to be the other
	 * way round, which left two windows with no way back:
	 *
	 *   - enqueue REJECTS. The exception left tick() entirely, so every remaining unit in the batch was
	 *     dropped -- permanently, because nextBatch persists the cursor past a unit before the unit is ever
	 *     handed over. One transient 500 from the execution queue silently lost the rest of the batch.
	 *   - enqueue SUCCEEDS and the row write then fails. The work is running in the execution queue with no
	 *     task row at all, so neither the completion path (which resolves by ExecRef) nor reconcile (which
	 *     reads in-flight rows) can ever see it. It runs, finishes, and the plan never learns either.
	 *
	 * Reserving first inverts both. A crash between the reservation and the hand-off leaves a Fed row with
	 * no ExecRef, which is exactly what reconcile now looks for; an orphaned EXECUTION is undetectable, an
	 * orphaned ROW is not. The cost is that a unit can be reserved and never dispatched, which reconcile
	 * retires the same way it retires any other interrupted task.
	 *
	 * Idempotent on re-feed: a unit whose row already exists reuses it rather than opening a second one for
	 * the same work, so a cursor that offers a unit twice after a crash cannot double-dispatch it.
	 *
	 * Returns true when the unit was handed over, false when it was reserved but the hand-off failed. Never
	 * throws: one failing unit must not abort a tick and strand the rest of its batch, which was the first
	 * of the two windows above.
	 */
	async _feedUnit(pScope, pGroup, pUnit)
	{
		let tmpTask = null;
		try
		{
			tmpTask = await this.Store.readTaskByUnitKey(pScope, pGroup.IDOperationGroup, pUnit.UnitKey);
			if (tmpTask && (tmpTask.Status === 'Fed' || tmpTask.Status === 'Running'))
			{
				// Already in flight from an earlier tick. Leave it alone; reconcile owns it now.
				return false;
			}
			// A classic unit is stored on its row while it is in flight. Its cursor has already moved past it, so the
			// row is the only place a restart can re-drive it from. A generative unit is re-driven from its frontier
			// node instead, so its row stays lean. Cleared again when the unit completes.
			let tmpStoredUnit = pGroup.Generative ? '' : this._packUnit(pUnit);
			if (!tmpTask)
			{
				let tmpRecord =
				{
					IDOperationGroup: pGroup.IDOperationGroup,
					UnitKey: pUnit.UnitKey,
					Lane: pUnit.Lane,
					Priority: pUnit.Priority,
					Status: 'Fed',
					ExecRef: ''
				};
				if (tmpStoredUnit) { tmpRecord.Params = tmpStoredUnit; }
				tmpTask = await this.Store.createTask(pScope, tmpRecord);
			}
			else
			{
				// A previously failed, reconciled or requeued row for this unit: reuse it rather than accumulating
				// one row per attempt, so the unit's history (and its Attempt count) stays on a single row.
				let tmpUpdate = { IDOperationTask: tmpTask.IDOperationTask, Status: 'Fed', ExecRef: '', LastError: '' };
				if (tmpStoredUnit) { tmpUpdate.Params = tmpStoredUnit; }
				await this.Store.updateTask(pScope, tmpUpdate);
			}
		}
		catch (pReserveError)
		{
			// Could not even reserve. Nothing was dispatched, so the unit is simply not fed this tick; the
			// consumer's dedup and cursor decide whether it comes back.
			this.log.warn('OperationQueue: could not reserve ' + pUnit.UnitKey + ' ('
				+ (pReserveError && pReserveError.message) + ').');
			return false;
		}

		try
		{
			let tmpExecRef = await this.ExecutionClient.enqueue(pUnit);
			await this.Store.updateTask(pScope,
				{ IDOperationTask: tmpTask.IDOperationTask, ExecRef: String(tmpExecRef) });
			return true;
		}
		catch (pEnqueueError)
		{
			// The row survives with no ExecRef, which is what makes this recoverable rather than silent.
			this.log.warn('OperationQueue: hand-off failed for ' + pUnit.UnitKey + ' ('
				+ (pEnqueueError && pEnqueueError.message) + '); its task row is reserved for reconcile.');
			return false;
		}
	}

	async tick()
	{
		if (!this.ExecutionClient) { throw new Error('No execution client set; call setExecutionClient() first.'); }

		let tmpOccupancy = await this.Store.countInflightTasks();
		let tmpRoom = this.WindowSize - tmpOccupancy;
		if (tmpRoom <= 0) { return { Fed: 0, Occupancy: tmpOccupancy, RoomLeft: 0, Errors: [] }; }

		// Highest lane/priority first. Groups are coarse (far fewer than tasks), so a JS sort is fine.
		let tmpGroups = (await this.Store.readGroupsByStatus('Active')).slice()
			.sort((pA, pB) => (pB.Priority || 0) - (pA.Priority || 0));

		let tmpFed = 0;
		let tmpErrors = [];
		for (let tmpGroup of tmpGroups)
		{
			if (tmpRoom <= 0) { break; }
			let tmpScope = { IDUser: tmpGroup.OwnerIDUser, IDCustomer: tmpGroup.IDCustomer };
			// Each group is fed in isolation. One group's enumerator throwing (a consumer bug, an unreadable source, a
			// store hiccup on its cursor) used to reject the whole tick, so every group behind it starved -- on every
			// pump, until the bad group was canceled. Now that group is skipped this tick, left Active, and retried.
			try
			{
				// Re-drive first: a classic unit a restart interrupted (Requeued by reconcileInflight) is older than
				// anything the cursor will offer next. It was counted in FedUnits when it was first fed.
				if (!tmpGroup.Generative)
				{
					let tmpRequeued = await this.Store.readRequeuedTasksForGroup(tmpScope, tmpGroup.IDOperationGroup, tmpRoom);
					for (let tmpTask of tmpRequeued)
					{
						if (tmpRoom <= 0) { break; }
						let tmpUnit = this._unpackUnit(tmpTask, tmpGroup);
						if (!tmpUnit)
						{
							await this._retireTask(tmpScope, tmpTask, parseInt(tmpTask.Attempt, 10) || 0, 'requeued without a stored unit to re-drive');
							continue;
						}
						if (await this._feedUnit(tmpScope, tmpGroup, tmpUnit)) { tmpFed++; }
						tmpRoom--;
					}
				}
				if (tmpRoom > 0)
				{
					// Pull at most `room` survivors so nothing is enumerated past the cursor without being fed.
					let tmpBatch = await this.Intent.nextBatch(tmpScope, tmpGroup, tmpRoom);
					for (let tmpUnit of tmpBatch.Units)
					{
						if (tmpRoom <= 0) { break; }
						let tmpFedUnit = await this._feedUnit(tmpScope, tmpGroup, tmpUnit);
						if (tmpFedUnit)
						{
							tmpGroup.FedUnits = (tmpGroup.FedUnits || 0) + 1;
							await this.Store.updateGroup(tmpScope, { IDOperationGroup: tmpGroup.IDOperationGroup, FedUnits: tmpGroup.FedUnits });
							tmpFed++;
						}
						// The slot is consumed either way. A unit whose hand-off failed holds a Fed row until
						// reconcile deals with it, so counting it here keeps the window honest.
						tmpRoom--;
					}
				}
				this._GroupErrors.delete(tmpGroup.IDOperationGroup + ':feed');
			}
			catch (pFeedError)
			{
				tmpErrors.push(this._groupError(tmpGroup, 'feed', pFeedError));
			}
		}

		// Reap any Active group that has fully drained. This catches the case a completion cannot -- a group
		// that fed nothing (an empty enumeration, or one already fully done) never produces an outcome to
		// trigger reportOutcome's reap. Progressing groups fall through cheaply on the in-flight check.
		for (let tmpGroup of tmpGroups)
		{
			try
			{
				await this._reapIfDone({ IDUser: tmpGroup.OwnerIDUser, IDCustomer: tmpGroup.IDCustomer }, tmpGroup);
				this._GroupErrors.delete(tmpGroup.IDOperationGroup + ':reap');
			}
			catch (pReapError)
			{
				tmpErrors.push(this._groupError(tmpGroup, 'reap', pReapError));
			}
		}
		return { Fed: tmpFed, Occupancy: tmpOccupancy + tmpFed, RoomLeft: tmpRoom, Errors: tmpErrors };
	}

	// A work item finished. Mark its task Done or Failed and bump the group's counts; the freed slot is
	// picked up by the next tick(). Unknown refs, and refs whose task is no longer live (reconciled, requeued,
	// canceled), are a no-op.
	async reportOutcome(pExecRef, pOutcome)
	{
		let tmpTask = await this.Store.readTaskByExecRef(String(pExecRef));
		if (!tmpTask) { return null; }
		let tmpScope = { IDUser: tmpTask.OwnerIDUser, IDCustomer: tmpTask.IDCustomer };
		let tmpSuccess = !!(pOutcome && pOutcome.Success);

		if (tmpSuccess)
		{
			let tmpDone = { IDOperationTask: tmpTask.IDOperationTask, Status: 'Done' };
			// The stored unit only matters while the unit might need re-driving; a Done row does not keep it.
			if (tmpTask.Params) { tmpDone.Params = ''; }
			await this.Store.updateTask(tmpScope, tmpDone);
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
		// A classic group's Requeued units (interrupted by a restart, not yet re-fed) are unfinished work even with
		// nothing in flight and the cursor exhausted. Reaping here would strand them, never run.
		if (!tmpGroup.Generative && (await this.Store.countRequeuedForGroup(pScope, tmpGroup.IDOperationGroup)) > 0) { return false; }
		let tmpExhausted = await this.Intent.isExpansionExhausted(pScope, tmpGroup);
		if (!tmpExhausted) { return false; }
		await this.Store.updateGroup(pScope, { IDOperationGroup: tmpGroup.IDOperationGroup, Status: 'Done', FinishedDate: new Date().toISOString() });
		let tmpDone = await this.Store.readGroup(pScope, tmpGroup.IDOperationGroup);
		this.Intent._fireGroupDone(pScope, tmpDone || tmpGroup);
		return true;
	}

	/**
	 * Restart reconcile: settle every task a crash left in flight (Fed/Running). Call once on boot, BEFORE pumping.
	 * Idempotent. Returns { Requeued, DeadLettered }.
	 *
	 * One policy for both kinds of group: RE-DRIVE the interrupted unit, up to a per-unit attempt cap, then
	 * dead-letter it Failed. A crash never silently drops work, and a unit that takes the process down every time
	 * cannot loop forever.
	 *
	 *   - GENERATIVE: the unit's frontier node goes back to pending and the next tick re-claims it. The attempt
	 *     count lives on the node, because a re-fed task starts from the node.
	 *   - CLASSIC: the cursor has already moved past the unit, so nothing will enumerate it again. It is re-driven
	 *     from the unit stored on its task row at reservation: the row goes Requeued, and the next tick re-feeds it
	 *     (ahead of new cursor work) with pUnit.Attempt set. The attempt count lives on the row, which every re-feed
	 *     reuses. A row with no stored unit (reserved by 0.5.1 or earlier) cannot be re-driven and retires Failed.
	 *
	 * Classic units used to retire Failed ('orphaned by restart') unconditionally, which LOST them: the group went
	 * on to reap Done having never run them. The cost of re-driving is that a unit whose interrupted attempt already
	 * had side effects may run again, so a consumer with non-idempotent work checks pUnit.Attempt.
	 *
	 * Last, walk claims with no task behind them are released (counted in Requeued): nextBatch marks a generative
	 * node 'walking' BEFORE its task row is reserved, so a crash between the two left a node nothing would ever
	 * complete, and the group could never drain.
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

			let tmpReason = null;
			if (!tmpGroup)
			{
				tmpReason = 'orphaned by restart (its group is gone)';
			}
			else if (tmpAttempt > tmpCap)
			{
				tmpReason = 'dead-lettered after ' + tmpCap + ' attempts';
			}
			else if (tmpGenerative)
			{
				if (tmpNode) { await this.Store.updateFrontier(tmpScope, { IDOperationFrontier: tmpNode.IDOperationFrontier, State: 'pending', Attempt: tmpAttempt }); }
				await this.Store.updateTask(tmpScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Reconciled', Attempt: tmpAttempt });
			}
			else if (this._unpackUnit(tmpTask, tmpGroup))
			{
				await this.Store.updateTask(tmpScope, { IDOperationTask: tmpTask.IDOperationTask, Status: 'Requeued', Attempt: tmpAttempt, ExecRef: '' });
			}
			else
			{
				tmpReason = 'orphaned by restart (no stored unit to re-drive)';
			}

			if (!tmpReason)
			{
				tmpRequeued++;
				continue;
			}
			if (tmpNode) { await this.Store.deleteFrontier(tmpNode.IDOperationFrontier); }
			await this._retireTask(tmpScope, tmpTask, tmpAttempt, tmpReason);
			tmpDead++;
		}
		tmpRequeued += await this._releaseOrphanedClaims();
		return { Requeued: tmpRequeued, DeadLettered: tmpDead };
	}

	// Release every generative walk claim with no live task behind it back to pending (see reconcileInflight). Runs
	// after the in-flight tasks were settled, so at boot every remaining 'walking' node is an orphan; the live-task
	// check keeps it safe if it is ever called while a pump is running. Returns the number released.
	async _releaseOrphanedClaims()
	{
		let tmpReleased = 0;
		let tmpGroups = (await this.Store.readGroupsByStatus('Active')).concat(await this.Store.readGroupsByStatus('Paused'));
		for (let tmpGroup of tmpGroups)
		{
			if (!tmpGroup.Generative) { continue; }
			let tmpScope = { IDUser: tmpGroup.OwnerIDUser, IDCustomer: tmpGroup.IDCustomer };
			let tmpWalking = await this.Store.readFrontierByState(tmpScope, tmpGroup.IDOperationGroup, 'walking');
			for (let tmpNode of tmpWalking)
			{
				let tmpTask = await this.Store.readTaskByUnitKey(tmpScope, tmpGroup.IDOperationGroup, tmpNode.NodeKey);
				if (tmpTask && (tmpTask.Status === 'Fed' || tmpTask.Status === 'Running')) { continue; }
				await this.Store.updateFrontier(tmpScope, { IDOperationFrontier: tmpNode.IDOperationFrontier, State: 'pending' });
				tmpReleased++;
			}
		}
		return tmpReleased;
	}

	// Retire a task that will not run again: Failed with a reason, counted against its group.
	async _retireTask(pScope, pTask, pAttempt, pReason)
	{
		await this.Store.updateTask(pScope, { IDOperationTask: pTask.IDOperationTask, Status: 'Failed', Attempt: pAttempt, LastError: pReason });
		await this._bumpGroupCount(pScope, pTask.IDOperationGroup, 'FailedUnits');
	}

	// Serialize a unit for its task row. Never throws: a unit that cannot be serialized simply cannot be re-driven.
	_packUnit(pUnit)
	{
		try { return JSON.stringify(pUnit) || ''; }
		catch (pError) { return ''; }
	}

	// The unit stored on a task row, restamped with its group's CURRENT lane and priority (it may have been
	// reprioritized since) and the row's attempt count. Null when the row holds no usable unit.
	_unpackUnit(pTask, pGroup)
	{
		let tmpUnit = pTask && pTask.Params;
		if (typeof tmpUnit === 'string')
		{
			if (!tmpUnit) { return null; }
			try { tmpUnit = JSON.parse(tmpUnit); }
			catch (pError) { return null; }
		}
		if (!tmpUnit || typeof tmpUnit !== 'object' || String(tmpUnit.UnitKey) !== String(pTask.UnitKey)) { return null; }
		return Object.assign(tmpUnit, { IDOperationGroup: pGroup.IDOperationGroup, Lane: pGroup.Lane, Priority: pGroup.Priority, Attempt: parseInt(pTask.Attempt, 10) || 0 });
	}

	// Record one group's tick failure: log it (once per distinct message, not on every pump) and describe it for
	// tick()'s Errors.
	_groupError(pGroup, pPhase, pError)
	{
		let tmpKey = pGroup.IDOperationGroup + ':' + pPhase;
		let tmpMessage = (pError && pError.message) || String(pError);
		if (this._GroupErrors.get(tmpKey) !== tmpMessage)
		{
			this._GroupErrors.set(tmpKey, tmpMessage);
			this.log.warn('OperationQueue: ' + pPhase + ' skipped group ' + pGroup.IDOperationGroup + ' [' + pGroup.OperationType
				+ '] this tick (' + tmpMessage + '); the other groups carry on.');
		}
		return { IDOperationGroup: pGroup.IDOperationGroup, Phase: pPhase, Error: tmpMessage };
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
