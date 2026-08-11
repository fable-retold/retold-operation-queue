/**
 * retold-operation-queue -- public entry.
 *
 * The intent/control layer over an execution queue (Ultravisor and friends): durable operation groups
 * that expand lazily into leaf tasks, lane-based preemptive priority, cache-key dedup, and metered
 * dispatch of a bounded window into a worker queue. This file wires the pieces; the store is the
 * Phase-A foundation. The scheduler + metering (Phase C) and the execution-client adapter (Phase D)
 * attach here as they land.
 */

const libFable = require('fable');

const OperationQueueStore = require('./Operation-Queue-Store.js');
const _MeadowModel = require('../data_model/compiled/OperationQueue-Extended.json');

/**
 * Create a self-contained instance (its own Fable + connected store) for standalone use and tests.
 * A host app that already has a Fable instead does:
 *   pFable.addAndInstantiateServiceType('OperationQueueStore', OperationQueueStore);
 *   pFable.OperationQueueStore.connect(fCallback);
 */
function createStandalone(pOptions, fCallback)
{
	// Default to a quiet log level (Fable defaults to trace, which dumps every CREATE TABLE); a caller
	// can override by passing its own LogStreams.
	let tmpOptions = Object.assign({ LogStreams: [ { level: 'warn' } ] }, pOptions || {});
	let tmpFable = new libFable(tmpOptions);
	if (tmpOptions.SQLiteFilePath)
	{
		if (!tmpFable.settings.SQLite) { tmpFable.settings.SQLite = {}; }
		tmpFable.settings.SQLite.SQLiteFilePath = tmpOptions.SQLiteFilePath;
	}
	tmpFable.addAndInstantiateServiceType('OperationQueueStore', OperationQueueStore);
	let tmpStore = tmpFable.OperationQueueStore;
	tmpStore.connect((pError) => fCallback(pError, tmpStore, tmpFable));
}

module.exports = { OperationQueueStore, MeadowModel: _MeadowModel, createStandalone };
