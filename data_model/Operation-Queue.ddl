!OperationGroup
>A unit of intended work: a folder, a browse-selection, or a library subtree a consumer wants
>operated on (for example, thumbnail every image under this path). It knows how many units it
>covers, but its leaf tasks are expanded lazily; the group carries the running counts, so a
>completed group IS the history (there is no per-task archive). Owner-scoped: the consumer stamps
>OwnerIDUser and IDCustomer from its session. Lane and Priority govern scheduling; a whole group is
>reprioritized or canceled as one.
@IDOperationGroup
%GUIDOperationGroup 36
&CreateDate
&UpdateDate
^Deleted
&DeleteDate
#CreatingIDUser
#OwnerIDUser
#IDCustomer
$ConsumerKey 64
$OperationType 64
$Label 400
$SourceRef 2048
$Lane 16
#Priority
$Status 16
#TotalUnits
#FedUnits
#CompletedUnits
#FailedUnits
#SkippedUnits
{Params
&StartedDate
&FinishedDate

!OperationTask
>A single leaf unit of a group (one file, one operation). Only tasks that are in-flight (fed to the
>execution queue) or terminally failed are persisted here; the vast pending backlog is never
>materialized, it is regenerated on demand from the group's enumerator and cursor. ExecRef holds the
>execution-queue work-item handle once fed, so status callbacks reconcile back to the task.
@IDOperationTask
%GUIDOperationTask 36
&CreateDate
&UpdateDate
^Deleted
&DeleteDate
#CreatingIDUser
#OwnerIDUser
#IDCustomer
#IDOperationGroup
$UnitKey 2048
$Lane 16
#Priority
$Status 16
$ExecRef 128
#Attempt
*LastError
{Params
&FedDate
&FinishedDate

!OperationCursor
>The resumable position of a group's lazy enumerator: where the next batch of leaf units picks up,
>so a huge group can be walked incrementally across restarts without ever listing it all at once.
>One row per group. CursorToken is opaque and consumer-defined (for example, the last path yielded).
@IDOperationCursor
%GUIDOperationCursor 36
&CreateDate
&UpdateDate
#OwnerIDUser
#IDCustomer
#IDOperationGroup
*CursorToken
^Exhausted
#YieldedCount
