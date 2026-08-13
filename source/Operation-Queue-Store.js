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
	 * Bring up the Meadow provider, ensure the tables exist, and build one Meadow DAL per entity.
	 * Idempotent: createTables is CREATE TABLE IF NOT EXISTS, and a second connect is a no-op.
	 *
	 * Provider resolution, in order:
	 *   1. ATTACH -- a Meadow provider the HOST already stood up on this Fable (MeadowPostgreSQLProvider or
	 *      MeadowSQLiteProvider). The queue then shares the app's database + connection, so its tables live
	 *      alongside the app's on one connection (the "everything on Postgres" topology).
	 *   2. STANDALONE -- none present, so bring our own up from settings.DatabaseProvider ('PostgreSQL' or
	 *      'SQLite', default SQLite), each reading its own settings block exactly like the Retold foundation.
	 * The connection modules are required lazily so a SQLite-only queue (the hermetic tests) never loads pg.
	 */
	connect(fCallback)
	{
		let tmpSelf = this;
		if (this.Connected) { return fCallback(); }

		let tmpProvider = this.fable.MeadowPostgreSQLProvider ? { Name: 'PostgreSQL', Service: 'MeadowPostgreSQLProvider' }
			: this.fable.MeadowSQLiteProvider ? { Name: 'SQLite', Service: 'MeadowSQLiteProvider' }
			: (String(this.fable.settings.DatabaseProvider) === 'PostgreSQL') ? { Name: 'PostgreSQL', Service: 'MeadowPostgreSQLProvider' }
			: { Name: 'SQLite', Service: 'MeadowSQLiteProvider' };

		let fEnsureProvider = (fNext) =>
		{
			if (tmpSelf.fable[tmpProvider.Service]) { return fNext(); }   // attach: the host already connected it
			if (tmpProvider.Name === 'PostgreSQL')
			{
				tmpSelf.fable.addAndInstantiateServiceType(tmpProvider.Service, require('meadow-connection-postgresql'));
			}
			else
			{
				let tmpFilePath = (tmpSelf.options && tmpSelf.options.SQLiteFilePath) ||
					(tmpSelf.fable.settings.SQLite && tmpSelf.fable.settings.SQLite.SQLiteFilePath) || ':memory:';
				if (!tmpSelf.fable.settings.SQLite) { tmpSelf.fable.settings.SQLite = {}; }
				tmpSelf.fable.settings.SQLite.SQLiteFilePath = tmpFilePath;
				tmpSelf.fable.addAndInstantiateServiceType(tmpProvider.Service, libMeadowConnectionSQLite);
			}
			tmpSelf.fable[tmpProvider.Service].connectAsync((pConnectError) => fNext(pConnectError));
		};

		fEnsureProvider((pConnectError) =>
		{
			if (pConnectError) { return fCallback(pConnectError); }
			tmpSelf.fable[tmpProvider.Service].createTables(tmpSelf.MeadowModel, (pTableError) =>
			{
				if (pTableError) { return fCallback(pTableError); }
				let tmpMeadow = libMeadow.new(tmpSelf.fable);
				Object.keys(tmpSelf.MeadowModel.Tables).forEach((pTableName) =>
				{
					let tmpDAL = tmpMeadow.loadFromPackageObject(tmpSelf.MeadowModel.Tables[pTableName].MeadowSchema);
					tmpDAL.setProvider(tmpProvider.Name);
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

	// A real SELECT COUNT(*) -- the number without ever marshaling the rows into JS. The scheduler's drain
	// checks run on every tick against a frontier/task tail that can be hundreds of thousands of rows, so a
	// count MUST NOT go through _reads (which loads every matched row just to take .length).
	_count(pTable, fBuildQuery)
	{
		return new Promise((fResolve, fReject) =>
		{
			let tmpDAL = this.DAL[pTable];
			if (!tmpDAL) { return fReject(new Error('No DAL for [' + pTable + '].')); }
			let tmpQuery = fBuildQuery ? fBuildQuery(tmpDAL.query.clone()) : tmpDAL.query.clone();
			tmpDAL.doCount(tmpQuery, (pError, pCountQuery, pCount) => (pError ? fReject(pError) : fResolve(pCount || 0)));
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

	// ---- OperationFrontier (the live wavefront of a generative group's walk) ----

	// Bulk-create pending frontier nodes for a group (a walk's newly-discovered directories/tree nodes).
	createFrontierNodes(pScope, pNodes)
	{
		let tmpSelf = this;
		return Promise.all((pNodes || []).map((pNode) =>
			tmpSelf._create('OperationFrontier', tmpSelf._own(pScope, Object.assign({ State: 'pending', Attempt: 0 }, pNode)), pScope.IDUser)));
	}

	// The next pending nodes for a group, oldest first (breadth-first-ish), capped to a batch. The cap
	// keeps a wide frontier from being fully read every tick; the slice is a belt in case a provider
	// ignores the cap.
	readPendingFrontier(pScope, pIDOperationGroup, pLimit)
	{
		return this._reads('OperationFrontier', (pQuery) =>
		{
			pQuery.addFilter('IDOperationGroup', pIDOperationGroup)
				.addFilter('OwnerIDUser', pScope.IDUser).addFilter('IDCustomer', pScope.IDCustomer)
				.addFilter('State', 'pending')
				.addSort({ Column: 'IDOperationFrontier', Direction: 'Ascending' });
			if (pLimit) { pQuery.setCap(pLimit); }
			return pQuery;
		}).then((pRows) =>
		{
			let tmpRows = pRows.filter((pRow) => !pRow.Deleted);
			return pLimit ? tmpRows.slice(0, pLimit) : tmpRows;
		});
	}

	// Find a group's active (not-yet-done) frontier node by its NodeKey (how a completion locates its node).
	getFrontierNode(pScope, pIDOperationGroup, pNodeKey)
	{
		return this._reads('OperationFrontier', (pQuery) => pQuery
			.addFilter('IDOperationGroup', pIDOperationGroup)
			.addFilter('OwnerIDUser', pScope.IDUser).addFilter('IDCustomer', pScope.IDCustomer)
			.addFilter('NodeKey', pNodeKey))
			.then((pRows) => pRows.filter((pRow) => !pRow.Deleted && pRow.State !== 'done')[0] || null);
	}

	updateFrontier(pScope, pNode) { return this._update('OperationFrontier', pNode, pScope.IDUser); }

	// Remove a frontier node once it is expanded/done, so the table holds ONLY the live wavefront -- a walk of
	// millions of directories keeps its current breadth here, not every directory ever, which keeps the
	// exhaustion + pending reads cheap. Best-effort: a delete failure is non-fatal (countFrontierActive still
	// ignores non-pending/walking rows).
	deleteFrontier(pIDOperationFrontier)
	{
		let tmpSelf = this;
		return new Promise((fResolve) =>
		{
			let tmpDAL = tmpSelf.DAL['OperationFrontier'];
			if (!tmpDAL) { return fResolve(); }
			// HARD delete (disableDeleteTracking) -- the row must physically leave the table, not soft-flag to a
			// tombstone, or a million-directory walk's frontier would grow without bound. It also keeps the prune
			// off the Deleted column entirely.
			let tmpQuery = tmpDAL.query.clone().addFilter('IDOperationFrontier', pIDOperationFrontier).setDisableDeleteTracking(true);
			tmpDAL.doDelete(tmpQuery, () => fResolve());
		});
	}

	// Count the still-active (pending or walking) frontier nodes of a group -- zero means the walk is done.
	// A COUNT per live state (never a SELECT *): a wide walk keeps hundreds of thousands of pending nodes here
	// and this runs on the scheduler's drain check, so it must not read the wavefront into JS.
	countFrontierActive(pScope, pIDOperationGroup)
	{
		let fScoped = (pState) => (pQuery) => pQuery
			.addFilter('IDOperationGroup', pIDOperationGroup)
			.addFilter('OwnerIDUser', pScope.IDUser).addFilter('IDCustomer', pScope.IDCustomer)
			.addFilter('Deleted', 0).addFilter('State', pState);
		return Promise.all(
			[
				this._count('OperationFrontier', fScoped('pending')),
				this._count('OperationFrontier', fScoped('walking'))
			]).then((pCounts) => pCounts[0] + pCounts[1]);
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

	// Count one group's in-flight (Fed/Running) tasks -- the scheduler's cheap "is this group still working?"
	// short-circuit. A COUNT per status, so the accumulating tail of Done/Failed task rows is never read into
	// JS just to find the handful still running. Any positive count means the group cannot be reaped yet.
	countInflightForGroup(pScope, pIDOperationGroup)
	{
		let fScoped = (pStatus) => (pQuery) => pQuery
			.addFilter('IDOperationGroup', pIDOperationGroup)
			.addFilter('OwnerIDUser', pScope.IDUser).addFilter('IDCustomer', pScope.IDCustomer)
			.addFilter('Deleted', 0).addFilter('Status', pStatus);
		return Promise.all(
			[
				this._count('OperationTask', fScoped('Fed')),
				this._count('OperationTask', fScoped('Running'))
			]).then((pCounts) => pCounts[0] + pCounts[1]);
	}

	// Every in-flight task with an ExecRef, across all owners (for reconciling against the execution queue).
	readInflightTasks()
	{
		return Promise.all(
			[
				this._reads('OperationTask', (pQuery) => pQuery.addFilter('Status', 'Fed')),
				this._reads('OperationTask', (pQuery) => pQuery.addFilter('Status', 'Running'))
			])
			.then((pResults) => pResults[0].concat(pResults[1]).filter((pRow) => !pRow.Deleted && pRow.ExecRef));
	}
}

module.exports = OperationQueueStore;
