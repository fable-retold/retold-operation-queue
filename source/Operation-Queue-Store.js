/**
 * Operation Queue Store: the module's own Meadow + Stricture persistence (SQLite today, Postgres later).
 *
 * Owns three entities -- OperationGroup, OperationTask, OperationCursor -- and nothing else. It is
 * deliberately consumer-agnostic: every row carries OwnerIDUser + IDCustomer, but the store does NOT
 * invent a tenancy model of its own. The consumer (Sluice, a test, ...) supplies the owner scope on
 * every call; the store stamps it on create and filters by it on read. Only in-flight and failed tasks
 * are ever materialized here -- the pending backlog lives in each group's lazy enumerator + cursor.
 */

const libFableServiceProviderBase = require('fable-serviceproviderbase');
const libMeadow = require('meadow');
const libMeadowConnectionSQLite = require('meadow-connection-sqlite');

const _MeadowModel = require('../data_model/compiled/OperationQueue-Extended.json');

class OperationQueueStore extends libFableServiceProviderBase
{
	constructor(pFable, pOptions, pServiceHash)
	{
		super(pFable, pOptions, pServiceHash);
		this.serviceType = 'OperationQueueStore';
		this.MeadowModel = _MeadowModel;
		this.DAL = {};
		this.Connected = false;
	}

	/**
	 * Bring up the SQLite provider, ensure the tables exist, and build one Meadow DAL per entity.
	 * Idempotent: createTables is CREATE TABLE IF NOT EXISTS, and a second connect is a no-op.
	 */
	connect(fCallback)
	{
		let tmpSelf = this;
		if (this.Connected) { return fCallback(); }

		let tmpFilePath = (this.options && this.options.SQLiteFilePath) ||
			(this.fable.settings.SQLite && this.fable.settings.SQLite.SQLiteFilePath) || ':memory:';
		if (!this.fable.settings.SQLite) { this.fable.settings.SQLite = {}; }
		this.fable.settings.SQLite.SQLiteFilePath = tmpFilePath;

		if (!this.fable.MeadowSQLiteProvider)
		{
			this.fable.addAndInstantiateServiceType('MeadowSQLiteProvider', libMeadowConnectionSQLite);
		}
		this.fable.MeadowSQLiteProvider.connectAsync((pConnectError) =>
		{
			if (pConnectError) { return fCallback(pConnectError); }
			tmpSelf.fable.MeadowSQLiteProvider.createTables(tmpSelf.MeadowModel, (pTableError) =>
			{
				if (pTableError) { return fCallback(pTableError); }
				let tmpMeadow = libMeadow.new(tmpSelf.fable);
				Object.keys(tmpSelf.MeadowModel.Tables).forEach((pTableName) =>
				{
					let tmpDAL = tmpMeadow.loadFromPackageObject(tmpSelf.MeadowModel.Tables[pTableName].MeadowSchema);
					tmpDAL.setProvider('SQLite');
					tmpSelf.DAL[pTableName] = tmpDAL;
				});
				tmpSelf.Connected = true;
				return fCallback();
			});
		});
	}

	// ---- low-level promise wrappers over one entity's DAL ----

	// pIDUser sets the query session user so Meadow stamps the audit columns (CreatingIDUser on create,
	// UpdatingIDUser on update) on tables that carry them; tables without those columns are unaffected.
	_create(pTable, pRecord, pIDUser)
	{
		return new Promise((fResolve, fReject) =>
		{
			let tmpDAL = this.DAL[pTable];
			if (!tmpDAL) { return fReject(new Error('No DAL for [' + pTable + ']; call connect() first.')); }
			let tmpQuery = tmpDAL.query.clone().addRecord(pRecord);
			if (tmpQuery.query && typeof pIDUser !== 'undefined') { tmpQuery.query.IDUser = pIDUser; }
			tmpDAL.doCreate(tmpQuery, (pError, pCreateQuery, pReadQuery, pRow) => (pError ? fReject(pError) : fResolve(pRow)));
		});
	}

	_update(pTable, pRecord, pIDUser)
	{
		return new Promise((fResolve, fReject) =>
		{
			let tmpDAL = this.DAL[pTable];
			if (!tmpDAL) { return fReject(new Error('No DAL for [' + pTable + '].')); }
			let tmpQuery = tmpDAL.query.clone().addRecord(pRecord);
			if (tmpQuery.query && typeof pIDUser !== 'undefined') { tmpQuery.query.IDUser = pIDUser; }
			tmpDAL.doUpdate(tmpQuery, (pError, pUpdateQuery, pRows) => (pError ? fReject(pError) : fResolve(pRows)));
		});
	}

	_reads(pTable, fBuildQuery)
	{
		return new Promise((fResolve, fReject) =>
		{
			let tmpDAL = this.DAL[pTable];
			if (!tmpDAL) { return fReject(new Error('No DAL for [' + pTable + '].')); }
			let tmpQuery = fBuildQuery ? fBuildQuery(tmpDAL.query.clone()) : tmpDAL.query.clone();
			tmpDAL.doReads(tmpQuery, (pError, pReadQuery, pRows) => (pError ? fReject(pError) : fResolve(pRows || [])));
		});
	}

	// Stamp owner scope onto a new record (the consumer supplies the scope). CreatingIDUser is NOT set
	// here: Meadow owns that audit column and fills it from the query session user (see _create).
	_own(pScope, pRecord)
	{
		let tmpRecord = Object.assign({}, pRecord);
		tmpRecord.OwnerIDUser = pScope.IDUser;
		tmpRecord.IDCustomer = pScope.IDCustomer;
		return tmpRecord;
	}

	// ---- OperationGroup ----

	createGroup(pScope, pGroup) { return this._create('OperationGroup', this._own(pScope, pGroup), pScope.IDUser); }
	updateGroup(pScope, pGroup) { return this._update('OperationGroup', pGroup, pScope.IDUser); }

	readGroup(pScope, pIDOperationGroup)
	{
		return this._reads('OperationGroup', (pQuery) => pQuery
			.addFilter('IDOperationGroup', pIDOperationGroup)
			.addFilter('OwnerIDUser', pScope.IDUser)
			.addFilter('IDCustomer', pScope.IDCustomer))
			.then((pRows) => pRows.filter((pRow) => !pRow.Deleted)[0] || null);
	}

	readGroups(pScope, fBuildQuery)
	{
		return this._reads('OperationGroup', (pQuery) =>
		{
			pQuery.addFilter('OwnerIDUser', pScope.IDUser).addFilter('IDCustomer', pScope.IDCustomer);
			return fBuildQuery ? fBuildQuery(pQuery) : pQuery;
		}).then((pRows) => pRows.filter((pRow) => !pRow.Deleted));
	}

	// ---- OperationTask (only in-flight + failed are materialized) ----

	createTask(pScope, pTask) { return this._create('OperationTask', this._own(pScope, pTask), pScope.IDUser); }
	updateTask(pScope, pTask) { return this._update('OperationTask', pTask, pScope.IDUser); }

	readTasks(pScope, fBuildQuery)
	{
		return this._reads('OperationTask', (pQuery) =>
		{
			pQuery.addFilter('OwnerIDUser', pScope.IDUser).addFilter('IDCustomer', pScope.IDCustomer);
			return fBuildQuery ? fBuildQuery(pQuery) : pQuery;
		}).then((pRows) => pRows.filter((pRow) => !pRow.Deleted));
	}

	// ---- OperationCursor (one per group) ----

	getCursor(pScope, pIDOperationGroup)
	{
		return this._reads('OperationCursor', (pQuery) => pQuery
			.addFilter('IDOperationGroup', pIDOperationGroup)
			.addFilter('OwnerIDUser', pScope.IDUser)
			.addFilter('IDCustomer', pScope.IDCustomer))
			.then((pRows) => pRows[0] || null);
	}

	upsertCursor(pScope, pCursor)
	{
		let tmpSelf = this;
		return this.getCursor(pScope, pCursor.IDOperationGroup).then((pExisting) =>
		{
			if (pExisting)
			{
				return tmpSelf._update('OperationCursor', Object.assign({}, pCursor, { IDOperationCursor: pExisting.IDOperationCursor }), pScope.IDUser);
			}
			return tmpSelf._create('OperationCursor', tmpSelf._own(pScope, pCursor), pScope.IDUser);
		});
	}

	// ---- system-level reads (no owner filter) for the scheduler infrastructure ----
	// The scheduler meters every owner's work into one shared execution window, so it needs an unscoped
	// view. Rows still carry their owner scope; the scheduler uses it when it acts on their behalf.

	// Count the in-flight tasks (the execution-window occupancy). Two equality reads keep this cheap even
	// as Done/Failed rows accumulate -- only Fed/Running rows are ever matched.
	countInflightTasks()
	{
		return Promise.all(
			[
				this._reads('OperationTask', (pQuery) => pQuery.addFilter('Status', 'Fed')),
				this._reads('OperationTask', (pQuery) => pQuery.addFilter('Status', 'Running'))
			])
			.then((pResults) => pResults[0].filter((pRow) => !pRow.Deleted).length + pResults[1].filter((pRow) => !pRow.Deleted).length);
	}

	// Every group in a given Status, across all owners (the scheduler sorts these by Priority).
	readGroupsByStatus(pStatus)
	{
		return this._reads('OperationGroup', (pQuery) => pQuery.addFilter('Status', pStatus))
			.then((pRows) => pRows.filter((pRow) => !pRow.Deleted));
	}

	// Resolve a task by its execution-queue handle (how a completion callback finds its task).
	readTaskByExecRef(pExecRef)
	{
		return this._reads('OperationTask', (pQuery) => pQuery.addFilter('ExecRef', pExecRef))
			.then((pRows) => pRows.filter((pRow) => !pRow.Deleted)[0] || null);
	}

	// The in-flight tasks of one group (owner-scoped), for group cancellation.
	readInflightTasksForGroup(pScope, pIDOperationGroup)
	{
		return this.readTasks(pScope, (pQuery) => pQuery.addFilter('IDOperationGroup', pIDOperationGroup))
			.then((pRows) => pRows.filter((pRow) => pRow.Status === 'Fed' || pRow.Status === 'Running'));
	}
}

module.exports = OperationQueueStore;
