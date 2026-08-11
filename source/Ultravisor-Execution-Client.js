/**
 * Ultravisor execution client: a concrete execution client for the scheduler, backed by an Ultravisor
 * coordinator reached through a fable-ultravisor-client (anything exposing request(method, path, body, cb)).
 *
 * It implements the scheduler's injected contract -- enqueue(pUnit) -> execRef, cancel(pExecRef) -- by
 * calling Ultravisor's ASYNC queue endpoints (POST /Beacon/Work/Enqueue, POST /Beacon/Work/:hash/Cancel),
 * so work flows through Ultravisor's scheduler rather than the synchronous /Dispatch path consumers use
 * today. The returned WorkItemHash is the execRef the scheduler stores on the task.
 *
 * Completion gets back to the scheduler two ways:
 *   - PUSH (primary in production): forward a queue.completed / queue.failed telemetry event to
 *     pScheduler.reportOutcome(hash, {Success, Error}). No code here is needed for that path.
 *   - reconcile() (fallback / no telemetry wired): poll each in-flight task's status and report the
 *     terminal ones. The status check is injected, so this module never hardcodes a status contract that
 *     drifts across Ultravisor versions.
 *
 * mapUnit(pUnit) turns a queue unit into an Ultravisor work-item body; the consumer supplies it, because
 * the mapping is operation-specific (which Capability/Action runs the op, and the payload). The default
 * passes the unit's Params through with its Priority.
 */

function _promisifyRequest(pClient, pMethod, pPath, pBody)
{
	return new Promise((fResolve, fReject) =>
		pClient.request(pMethod, pPath, pBody, (pError, pResult) => (pError ? fReject(pError) : fResolve(pResult))));
}

function _defaultMapUnit(pUnit)
{
	return {
		Capability: pUnit.Capability || 'Shell',
		Action: pUnit.Action || 'Execute',
		Priority: pUnit.Priority,
		Settings: pUnit.Params || { UnitKey: pUnit.UnitKey }
	};
}

// Ultravisor's enqueue response has carried the handle under a couple of names across versions; accept them.
function _extractHash(pResult)
{
	if (!pResult) { return null; }
	return pResult.WorkItemHash || pResult.Hash || (pResult.WorkItem && pResult.WorkItem.WorkItemHash) || null;
}

class UltravisorExecutionClient
{
	constructor(pOptions)
	{
		let tmpOptions = pOptions || {};
		this.Client = tmpOptions.Client || null; // a fable-ultravisor-client, or anything with request()
		this.MapUnit = (typeof tmpOptions.MapUnit === 'function') ? tmpOptions.MapUnit : _defaultMapUnit;
		this.CancelReason = tmpOptions.CancelReason || 'canceled by operation-queue';
	}

	setClient(pClient) { this.Client = pClient; return this; }

	// Scheduler contract: hand a unit to Ultravisor's async queue; return its WorkItemHash as the execRef.
	enqueue(pUnit)
	{
		if (!this.Client) { return Promise.reject(new Error('No Ultravisor client set; pass { Client } or call setClient().')); }
		let tmpBody = this.MapUnit(pUnit);
		return _promisifyRequest(this.Client, 'POST', '/Beacon/Work/Enqueue', tmpBody).then((pResult) =>
		{
			let tmpHash = _extractHash(pResult);
			if (!tmpHash) { throw new Error('Ultravisor enqueue returned no work-item hash.'); }
			return tmpHash;
		});
	}

	// Scheduler contract: cancel an in-flight item by its execRef (the WorkItemHash).
	cancel(pExecRef)
	{
		if (!this.Client) { return Promise.reject(new Error('No Ultravisor client set.')); }
		return _promisifyRequest(this.Client, 'POST', '/Beacon/Work/' + encodeURIComponent(pExecRef) + '/Cancel', { Reason: this.CancelReason });
	}

	/**
	 * Fallback completion path: reconcile the scheduler's in-flight tasks against Ultravisor. For each,
	 * fResolveStatus(pClient, pExecRef) -> { Terminal, Success, Error } (or a Promise of it) decides whether
	 * it finished; terminal ones are reported to the scheduler. Returns the number of outcomes reported.
	 */
	async reconcile(pScheduler, fResolveStatus)
	{
		if (typeof fResolveStatus !== 'function') { throw new Error('reconcile requires a status resolver function.'); }
		let tmpInflight = await pScheduler.Store.readInflightTasks();
		let tmpReported = 0;
		for (let tmpTask of tmpInflight)
		{
			let tmpStatus = await fResolveStatus(this.Client, tmpTask.ExecRef);
			if (tmpStatus && tmpStatus.Terminal)
			{
				await pScheduler.reportOutcome(tmpTask.ExecRef, { Success: !!tmpStatus.Success, Error: tmpStatus.Error });
				tmpReported++;
			}
		}
		return tmpReported;
	}
}

module.exports = UltravisorExecutionClient;
