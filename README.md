# retold-operation-queue

The intent and control layer over an execution queue.

An execution queue (for example, Ultravisor's work-dispatch queue) answers "what is running now": a
bounded, live window of work. retold-operation-queue answers "everything we intend to do": the
durable plan behind that window. It holds operation groups that expand lazily into leaf tasks,
governs their order with preemptive lanes, dedups against a cache, and meters a bounded window of
work into the execution queue -- so a backlog of millions of operations stays tractable and stays
cheap to reprioritize or cancel.

It is built for browse-driven work at scale: a person browsing a large library queues up huge
amounts of background work (thumbnail this folder, probe that video), reshapes its priority
interactively ("this folder is interesting, do it first"), and needs to see and control it.

## Status

Early. This release is the persistence foundation: the module's own Meadow + Stricture store for
operation groups, tasks, and cursors, with owner-scoping stamped and enforced. The scheduler, lane
metering, enumerator-driven lazy expansion, and the execution-client adapter land in subsequent
releases.

## The model

- Group: a unit of intended work (a folder, a selection, a subtree). It knows how many units it
  covers but expands into leaf tasks lazily through a resumable cursor, so a group of millions of
  files is never materialized as millions of rows.
- Lane: Urgent, Normal, or Background. Priority is set on the group and inherited by its tasks;
  raising a group's lane preempts lower-priority work.
- Task: one leaf unit. Only in-flight and terminally failed tasks are persisted; the pending
  backlog is regenerated on demand from the group's enumerator and cursor.
- Metering: a bounded window of work is fed into the execution queue and refilled as it drains, so
  the execution queue only ever holds "what is running now".

Owner scope (OwnerIDUser + IDCustomer) is carried on every row and supplied by the consumer; the
module does not impose a tenancy model of its own.

## Restarts and failures

- Call `Scheduler.reconcileInflight(attemptCap)` once at boot, before the first `tick()`. Every task
  a crash left in flight (Fed/Running) is re-driven, up to `attemptCap` attempts per unit, then
  dead-lettered Failed with a reason.
  - A generative unit's frontier node goes back to pending.
  - A classic unit is re-fed from the copy of the unit stored on its task row when it was reserved
    (its cursor is already past it). Re-fed units go ahead of new cursor work, and the group is not
    reaped Done until they have run. Rows reserved by 0.5.1 or earlier have no stored unit and
    retire Failed.
  - Walk claims left with no task behind them are released.
- A re-driven unit reaches `enqueue` with `unit.Attempt > 0`. Re-driving means an interrupted
  attempt can run again, so a consumer with non-idempotent work should check whether that attempt
  already got as far as its side effects.
- ExecRefs must be unique across restarts, not only within one process. Finished task rows are kept,
  and `reportOutcome` resolves only a live (Fed/Running) task. A consumer's in-process client should
  put a per-boot nonce in its refs.
- `tick()` feeds each group in isolation. A group whose enumerator throws is skipped for that tick,
  left Active, and reported in the result's `Errors`. The other groups are still fed.

## Install

    npm install retold-operation-queue

Requires Node 22 or newer (it uses the built-in SQLite connection through meadow-connection-sqlite).

## Usage

    const { createStandalone } = require('retold-operation-queue');

    createStandalone({ SQLiteFilePath: './operation-queue.sqlite' }, (pError, pStore) =>
    {
        if (pError) { throw pError; }
        let tmpScope = { IDUser: 7, IDCustomer: 3 };
        pStore.createGroup(tmpScope,
            {
                ConsumerKey: 'sluice', OperationType: 'rendition.thumbnail',
                Label: 'Thumbnail /Photos/2019', SourceRef: '2/Photos/2019',
                Lane: 'Background', Status: 'Planned', TotalUnits: 240000
            })
            .then((pGroup) => console.log('queued group', pGroup.IDOperationGroup));
    });

A host application that already runs Fable can register the store as a service instead:

    const { OperationQueueStore } = require('retold-operation-queue');
    pFable.addAndInstantiateServiceType('OperationQueueStore', OperationQueueStore);
    pFable.OperationQueueStore.connect(fCallback);

## Entities

- OperationGroup: the durable plan unit (lane, priority, status, running counts).
- OperationTask: a materialized in-flight or failed leaf unit.
- OperationCursor: the resumable enumerator position for a group (one per group).

## License

MIT. Part of the Retold ecosystem.
