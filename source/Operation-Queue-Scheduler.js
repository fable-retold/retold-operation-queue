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
		return tmpTask.IDOperationTask;
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
