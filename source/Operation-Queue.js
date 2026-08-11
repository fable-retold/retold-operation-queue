/**
 * retold-operation-queue -- public entry.
 *
 * The intent/control layer over an execution queue (Ultravisor and friends): durable operation groups
 * that expand lazily into leaf tasks, lane-based preemptive priority, cache-key dedup, and metered
 * dispatch of a bounded window into a worker queue. This file wires the pieces. The store (Phase A) is
 * the persistence foundation; the intent engine (Phase B) is the group/lane/lazy-expansion model on top.
 * The scheduler + metering (Phase C) and the execution-client adapter (Phase D) attach here as they land.
 */

const libFable = require('fable');

const OperationQueueStore = require('./Operation-Queue-Store.js');
const OperationQueueIntent = require('./Operation-Queue-Intent.js');
const OperationQueueScheduler = require('./Operation-Queue-Scheduler.js');
const libLanes = require('./Operation-Queue-Lanes.js');
const _MeadowModel = require('../data_model/compiled/OperationQueue-Extended.json');

/**
 * Create a self-contained instance (its own Fable + connected store + intent engine) for standalone
 * use and tests. The callback receives the intent engine; the store is available as engine.Store.
 *
 * A host application that already runs Fable does this itself:
 *   pFable.addAndInstantiateServiceType('OperationQueueStore', OperationQueueStore);
 *   pFable.addAndInstantiateServiceType('OperationQueueIntent', OperationQueueIntent);
 *   pFable.OperationQueueIntent.setStore(pFable.OperationQueueStore);
 *   pFable.OperationQueueStore.connect(fCallback);
 */
function createStandalone(pOptions, fCallback)
{
	let tmpOptions = Object.assign({ LogStreams: [ { level: 'warn' } ] }, pOptions || {});
	let tmpFable = new libFable(tmpOptions);
	if (tmpOptions.SQLiteFilePath)
	{
		if (!tmpFable.settings.SQLite) { tmpFable.settings.SQLite = {}; }
		tmpFable.settings.SQLite.SQLiteFilePath = tmpOptions.SQLiteFilePath;
	}
	tmpFable.addAndInstantiateServiceType('OperationQueueStore', OperationQueueStore);
	tmpFable.addAndInstantiateServiceType('OperationQueueIntent', OperationQueueIntent);
	tmpFable.addAndInstantiateServiceType('OperationQueueScheduler', OperationQueueScheduler);
	let tmpStore = tmpFable.OperationQueueStore;
	let tmpIntent = tmpFable.OperationQueueIntent;
	let tmpScheduler = tmpFable.OperationQueueScheduler;
	tmpIntent.setStore(tmpStore);
	tmpScheduler.setIntent(tmpIntent);
	if (tmpOptions.WindowSize) { tmpScheduler.WindowSize = tmpOptions.WindowSize; }
	// The intent engine is the returned handle; the store and scheduler hang off it.
	tmpIntent.Scheduler = tmpScheduler;
	tmpStore.connect((pError) => fCallback(pError, tmpIntent, tmpFable));
}

module.exports = { OperationQueueStore, OperationQueueIntent, OperationQueueScheduler, Lanes: libLanes, MeadowModel: _MeadowModel, createStandalone };
