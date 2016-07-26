/* ------------------------------------------------------------------------
 *
 * pl_funcs.c
 *		Utility C functions for stored procedures
 *
 * Copyright (c) 2015-2016, Postgres Professional
 *
 * ------------------------------------------------------------------------
 */

#include "pathman.h"
#include "access/nbtree.h"
#include "access/xact.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/array.h"
#include "utils/memutils.h"
#include "utils.h"
#include "funcapi.h"
#include "parser/parse_coerce.h"

static Datum cast_datum(Datum src, Oid src_type, Oid target_type);

/* declarations */
PG_FUNCTION_INFO_V1( on_partitions_created );
PG_FUNCTION_INFO_V1( on_partitions_updated );
PG_FUNCTION_INFO_V1( on_partitions_removed );
PG_FUNCTION_INFO_V1( on_enable_parent );
PG_FUNCTION_INFO_V1( on_disable_parent );
PG_FUNCTION_INFO_V1( find_or_create_range_partition );
PG_FUNCTION_INFO_V1( get_range_partition_by_idx );
PG_FUNCTION_INFO_V1( get_range_partition_by_oid );
PG_FUNCTION_INFO_V1( acquire_partitions_lock );
PG_FUNCTION_INFO_V1( release_partitions_lock );
PG_FUNCTION_INFO_V1( check_overlap );
PG_FUNCTION_INFO_V1( get_type_hash_func );
PG_FUNCTION_INFO_V1( get_hash );

/* pathman_range type */
typedef struct PathmanRange
{
	Oid			type_oid;
	bool		by_val;
	RangeEntry	range;
} PathmanRange;

typedef struct PathmanHash
{
	Oid			child_oid;
	uint32		hash;
} PathmanHash;

typedef struct PathmanRangeListCtxt
{
	Oid			type_oid;
	bool		by_val;
	RangeEntry *ranges;
	int			nranges;
	int			pos;
} PathmanRangeListCtxt;

PG_FUNCTION_INFO_V1( pathman_range_in );
PG_FUNCTION_INFO_V1( pathman_range_out );
PG_FUNCTION_INFO_V1( get_whole_range );
PG_FUNCTION_INFO_V1( range_partitions_list );
PG_FUNCTION_INFO_V1( range_lower );
PG_FUNCTION_INFO_V1( range_upper );
PG_FUNCTION_INFO_V1( range_oid );
PG_FUNCTION_INFO_V1( range_value_cmp );

/*
 * Casts datum to target type
 */
static Datum
cast_datum(Datum src, Oid src_type, Oid target_type)
{
	Oid			castfunc;
				CoercionPathType ctype;

	ctype = find_coercion_pathway(target_type, src_type,
								  COERCION_EXPLICIT,
								  &castfunc);
	if (ctype == COERCION_PATH_FUNC && OidIsValid(castfunc))
		return OidFunctionCall1(castfunc, src);

	/* TODO !!! */
	return 0;
}

/*
 * Partition-related operation type.
 */
typedef enum
{
	EV_ON_PART_CREATED = 1,
	EV_ON_PART_UPDATED,
	EV_ON_PART_REMOVED
} part_event_type;

/*
 * We have to reset shared memory cache each time a transaction
 * containing a partitioning-related operation has been rollbacked,
 * hence we need to pass a partitioned table's Oid & some other stuff.
 *
 * Note: 'relname' cannot be fetched within
 * Xact callbacks, so we have to store it here.
 */
typedef struct part_abort_arg part_abort_arg;

struct part_abort_arg
{
	Oid					partitioned_table_relid;
	char			   *relname;

	bool				is_subxact;		/* needed for correct callback removal */
	SubTransactionId	subxact_id;		/* necessary for detecting specific subxact */
	part_abort_arg	   *xact_cb_arg;	/* points to the parent Xact's arg */

	part_event_type		event;			/* created | updated | removed partitions */

	bool				expired;		/* set by (Sub)Xact when a job is done */
};


static part_abort_arg * make_part_abort_arg(Oid partitioned_table,
											part_event_type event,
											bool is_subxact,
											part_abort_arg *xact_cb_arg);

static void handle_part_event_cancellation(const part_abort_arg *arg);
static void on_xact_abort_callback(XactEvent event, void *arg);
static void on_subxact_abort_callback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);

static void remove_on_xact_abort_callbacks(void *arg);
static void add_on_xact_abort_callbacks(Oid partitioned_table, part_event_type event);

static void on_partitions_created_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks);
static void on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks);


/* Construct part_abort_arg for callbacks in TopTransactionContext. */
static part_abort_arg *
make_part_abort_arg(Oid partitioned_table, part_event_type event,
					bool is_subxact, part_abort_arg *xact_cb_arg)
{
	part_abort_arg *arg = MemoryContextAlloc(TopTransactionContext,
											 sizeof(part_abort_arg));

	const char	   *relname = get_rel_name(partitioned_table);

	/* Fill in Oid & relation name */
	arg->partitioned_table_relid = partitioned_table;
	arg->relname = MemoryContextStrdup(TopTransactionContext, relname);
	arg->is_subxact = is_subxact;
	arg->subxact_id = GetCurrentSubTransactionId(); /* for SubXact callback */
	arg->xact_cb_arg = xact_cb_arg;
	arg->event = event;
	arg->expired = false;

	return arg;
}

/* Revert shared memory cache changes iff xact has been aborted. */
static void
handle_part_event_cancellation(const part_abort_arg *arg)
{
#define DO_NOT_USE_CALLBACKS false /* just to clarify intentions */

	switch (arg->event)
	{
		case EV_ON_PART_CREATED:
			{
				elog(WARNING, "Partitioning of table '%s' has been aborted, "
							  "removing partitions from pg_pathman's cache",
					 arg->relname);

				on_partitions_removed_internal(arg->partitioned_table_relid,
											   DO_NOT_USE_CALLBACKS);
			}
			break;

		case EV_ON_PART_UPDATED:
			{
				elog(WARNING, "All changes in partitioned table "
							  "'%s' will be discarded",
					 arg->relname);

				on_partitions_updated_internal(arg->partitioned_table_relid,
											   DO_NOT_USE_CALLBACKS);
			}
			break;

		case EV_ON_PART_REMOVED:
			{
				elog(WARNING, "All changes in partitioned table "
							  "'%s' will be discarded",
					 arg->relname);

				on_partitions_created_internal(arg->partitioned_table_relid,
											   DO_NOT_USE_CALLBACKS);
			}
			break;

		default:
			elog(ERROR, "Unknown event spotted in xact callback");
	}
}

/*
 * Add & remove xact callbacks
 */

static void
remove_on_xact_abort_callbacks(void *arg)
{
	part_abort_arg *parg = (part_abort_arg *) arg;

	elog(DEBUG2, "remove_on_xact_abort_callbacks() "
				 "[is_subxact = %s, relname = '%s', event = %u] "
				 "triggered for relation %u",
		 (parg->is_subxact ? "true" : "false"), parg->relname,
		 parg->event, parg->partitioned_table_relid);

	/* Is this a SubXact callback or not? */
	if (!parg->is_subxact)
		UnregisterXactCallback(on_xact_abort_callback, arg);
	else
		UnregisterSubXactCallback(on_subxact_abort_callback, arg);

	pfree(arg);
}

static void
add_on_xact_abort_callbacks(Oid partitioned_table, part_event_type event)
{
	part_abort_arg *xact_cb_arg = make_part_abort_arg(partitioned_table,
													  event, false, NULL);

	RegisterXactCallback(on_xact_abort_callback, (void *) xact_cb_arg);
	execute_on_xact_mcxt_reset(TopTransactionContext,
							   remove_on_xact_abort_callbacks,
							   xact_cb_arg);

	/* Register SubXact callback if necessary */
	if (IsSubTransaction())
	{
		/*
		 * SubXact callback's arg contains a pointer to the parent
		 * Xact callback's arg. This will allow it to 'expire' both
		 * args and to prevent Xact's callback from doing anything
		 */
		void *subxact_cb_arg = make_part_abort_arg(partitioned_table, event,
												   true, xact_cb_arg);

		RegisterSubXactCallback(on_subxact_abort_callback, subxact_cb_arg);
		execute_on_xact_mcxt_reset(CurTransactionContext,
								   remove_on_xact_abort_callbacks,
								   subxact_cb_arg);
	}
}

/*
 * Xact & SubXact callbacks
 */

static void
on_xact_abort_callback(XactEvent event, void *arg)
{
	part_abort_arg *parg = (part_abort_arg *) arg;

	/* Check that this is an aborted Xact & action has not expired yet */
	if ((event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT) &&
		!parg->expired)
	{
		handle_part_event_cancellation(parg);

		/* Set expiration flag */
		parg->expired = true;
	}
}

static void
on_subxact_abort_callback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	part_abort_arg *parg = (part_abort_arg *) arg;

	Assert(parg->subxact_id != InvalidSubTransactionId);

	/* Check if this is an aborted SubXact we've been waiting for */
	if (event == SUBXACT_EVENT_ABORT_SUB &&
		mySubid <= parg->subxact_id && !parg->expired)
	{
		handle_part_event_cancellation(parg);

		/* Now set expiration flags to disable Xact callback */
		parg->xact_cb_arg->expired = true;
		parg->expired = true;
	}
}

/*
 * Callbacks
 */

static void
on_partitions_created_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_created() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
	load_relations(false);
	LWLockRelease(pmstate->load_config_lock);

	/* Register hooks that will clear shmem cache if needed */
	if (add_callbacks)
		add_on_xact_abort_callbacks(partitioned_table, EV_ON_PART_CREATED);
}

static void
on_partitions_updated_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_updated() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	if (get_pathman_relation_info(partitioned_table, NULL))
	{
		LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
			remove_relation_info(partitioned_table);
			load_relations(false);
		LWLockRelease(pmstate->load_config_lock);
	}

	/* Register hooks that will clear shmem cache if needed */
	if (add_callbacks)
		add_on_xact_abort_callbacks(partitioned_table, EV_ON_PART_UPDATED);
}

static void
on_partitions_removed_internal(Oid partitioned_table, bool add_callbacks)
{
	elog(DEBUG2, "on_partitions_removed() [add_callbacks = %s] "
				 "triggered for relation %u",
		 (add_callbacks ? "true" : "false"), partitioned_table);

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);
		remove_relation_info(partitioned_table);
	LWLockRelease(pmstate->load_config_lock);

	/* Register hooks that will clear shmem cache if needed */
	if (add_callbacks)
		add_on_xact_abort_callbacks(partitioned_table, EV_ON_PART_REMOVED);
}

/*
 * Thin layer between pure c and pl/PgSQL
 */

Datum
on_partitions_created(PG_FUNCTION_ARGS)
{
	on_partitions_created_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_updated(PG_FUNCTION_ARGS)
{
	on_partitions_updated_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_partitions_removed(PG_FUNCTION_ARGS)
{
	on_partitions_removed_internal(PG_GETARG_OID(0), true);
	PG_RETURN_NULL();
}

Datum
on_enable_parent(PG_FUNCTION_ARGS)
{
	Oid		relid = DatumGetObjectId(PG_GETARG_DATUM(0));
	PartRelationInfo   *prel;

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);

	prel = get_pathman_relation_info(relid, NULL);
	if (!prel)
		elog(ERROR, "Relation %s isn't handled by pg_pathman", get_rel_name(relid));
	prel->enable_parent = true;

	LWLockRelease(pmstate->load_config_lock);

	PG_RETURN_NULL();
}

Datum
on_disable_parent(PG_FUNCTION_ARGS)
{
	Oid		relid = DatumGetObjectId(PG_GETARG_DATUM(0));
	PartRelationInfo   *prel;

	LWLockAcquire(pmstate->load_config_lock, LW_EXCLUSIVE);

	prel = get_pathman_relation_info(relid, NULL);
	if (!prel)
		elog(ERROR, "Relation %s isn't handled by pg_pathman", get_rel_name(relid));
	prel->enable_parent = false;

	LWLockRelease(pmstate->load_config_lock);

	PG_RETURN_NULL();
}

/*
 * Returns partition oid for specified parent relid and value.
 * In case when partition isn't exist try to create one.
 */
Datum
find_or_create_range_partition(PG_FUNCTION_ARGS)
{
	Oid					relid = PG_GETARG_OID(0);
	Datum				value = PG_GETARG_DATUM(1);
	Oid					value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	RangeRelation	   *rangerel;
	PartRelationInfo   *prel;
	FmgrInfo			cmp_func;
	search_rangerel_result search_state;
	RangeEntry			found_re;

	prel = get_pathman_relation_info(relid, NULL);
	rangerel = get_pathman_range_relation(relid, NULL);

	if (!prel || !rangerel)
		PG_RETURN_NULL();

	fill_type_cmp_fmgr_info(&cmp_func, value_type, prel->atttype);

	search_state = search_range_partition_eq(value, &cmp_func,
											 rangerel, &found_re);

	/*
	 * If found then just return oid, else create new partitions
	 */
	if (search_state == SEARCH_RANGEREL_FOUND)
		PG_RETURN_OID(found_re.child_oid);
	/*
	 * If not found and value is between first and last partitions
	 */
	else if (search_state == SEARCH_RANGEREL_GAP)
		PG_RETURN_NULL();
	else
	{
		Oid		child_oid;

		/*
		 * Check if someone else has already created partition.
		 */
		search_state = search_range_partition_eq(value, &cmp_func,
												 rangerel, &found_re);
		if (search_state == SEARCH_RANGEREL_FOUND)
		{
			PG_RETURN_OID(found_re.child_oid);
		}

		/* Start background worker to create new partitions */
		child_oid = create_partitions_bg_worker(relid, value, value_type);
		// child_oid = create_partitions(relid, value, value_type, NULL);

		PG_RETURN_OID(child_oid);
	}
}

/*
 * Returns range (min, max) as output parameters
 *
 * first argument is the parent relid
 * second is the partition relid
 * third and forth are MIN and MAX output parameters
 */
Datum
get_range_partition_by_oid(PG_FUNCTION_ARGS)
{
	Oid					parent_oid = PG_GETARG_OID(0);
	Oid					child_oid = PG_GETARG_OID(1);
	int					i;
	bool				found = false;
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;

	prel = get_pathman_relation_info(parent_oid, NULL);

	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges, true);

	/* Looking for specified partition */
	for (i = 0; i < rangerel->ranges.elem_count; i++)
		if (ranges[i].child_oid == child_oid)
		{
			found = true;
			break;
		}

	if (found)
	{
		PathmanRange *rng = (PathmanRange *) palloc(sizeof(PathmanRange));

		rng->type_oid = prel->atttype;
		rng->by_val = rangerel->by_val;
		rng->range = ranges[i];

		PG_RETURN_POINTER(rng);
	}
	pfree(ranges);

	PG_RETURN_NULL();
}


/*
 * Returns N-th range (in form of array)
 *
 * First argument is the parent relid.
 * Second argument is the index of the range (if it is negative then the last
 * range will be returned).
 */
Datum
get_range_partition_by_idx(PG_FUNCTION_ARGS)
{
	Oid					parent_oid = PG_GETARG_OID(0);
	int					idx = PG_GETARG_INT32(1);
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;
	RangeEntry			re;
	PathmanRange	   *rng;

	prel = get_pathman_relation_info(parent_oid, NULL);

	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || idx >= (int)rangerel->ranges.elem_count)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges, true);
	if (idx >= 0)
		re = ranges[idx];
	else
		re = ranges[rangerel->ranges.elem_count - 1];

	rng = (PathmanRange *) palloc(sizeof(PathmanRange));
	rng->type_oid = prel->atttype;
	rng->by_val = rangerel->by_val;
	rng->range = re;

	pfree(ranges);

	PG_RETURN_POINTER(rng);
}

/*
 * Checks if range overlaps with existing partitions.
 * Returns TRUE if overlaps and FALSE otherwise.
 */
Datum
check_overlap(PG_FUNCTION_ARGS)
{
	Oid					partitioned_table = PG_GETARG_OID(0);

	Datum				p1 = PG_GETARG_DATUM(1),
						p2 = PG_GETARG_DATUM(2);

	Oid					p1_type = get_fn_expr_argtype(fcinfo->flinfo, 1),
						p2_type = get_fn_expr_argtype(fcinfo->flinfo, 2);

	FmgrInfo			cmp_func_1,
						cmp_func_2;

	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;
	int					i;
	bool				byVal;

	prel = get_pathman_relation_info(partitioned_table, NULL);
	rangerel = get_pathman_range_relation(partitioned_table, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE)
		PG_RETURN_NULL();

	/* comparison functions */
	fill_type_cmp_fmgr_info(&cmp_func_1, p1_type, prel->atttype);
	fill_type_cmp_fmgr_info(&cmp_func_2, p2_type, prel->atttype);

	byVal = rangerel->by_val;
	ranges = (RangeEntry *) dsm_array_get_pointer(&rangerel->ranges, true);
	for (i = 0; i < rangerel->ranges.elem_count; i++)
	{
		int c1 = FunctionCall2(&cmp_func_1, p1,
							   PATHMAN_GET_DATUM(ranges[i].max, byVal));
		int c2 = FunctionCall2(&cmp_func_2, p2,
							   PATHMAN_GET_DATUM(ranges[i].min, byVal));

		if (c1 < 0 && c2 > 0)
		{
			pfree(ranges);
			PG_RETURN_BOOL(true);
		}
	}

	pfree(ranges);
	PG_RETURN_BOOL(false);
}

/*
 * Acquire partitions lock
 */
Datum
acquire_partitions_lock(PG_FUNCTION_ARGS)
{
	LWLockAcquire(pmstate->edit_partitions_lock, LW_EXCLUSIVE);
	PG_RETURN_NULL();
}

Datum
release_partitions_lock(PG_FUNCTION_ARGS)
{
	LWLockRelease(pmstate->edit_partitions_lock);
	PG_RETURN_NULL();
}

/*
 * Returns hash function OID for specified type
 */
Datum
get_type_hash_func(PG_FUNCTION_ARGS)
{
	TypeCacheEntry *tce;
	Oid 			type_oid = PG_GETARG_OID(0);

	tce = lookup_type_cache(type_oid, TYPECACHE_HASH_PROC);

	PG_RETURN_OID(tce->hash_proc);
}

Datum
pathman_range_in(PG_FUNCTION_ARGS)
{
	elog(ERROR, "Not implemented");
}

Datum
pathman_range_out(PG_FUNCTION_ARGS)
{
	PathmanRange *rng = (PathmanRange *) PG_GETARG_POINTER(0);
	char	   *result;
	char	   *left,
			   *right;
	Oid			outputfunc;
	bool		typisvarlena;

	getTypeOutputInfo(rng->type_oid, &outputfunc, &typisvarlena);
	left = OidOutputFunctionCall(outputfunc, PATHMAN_GET_DATUM(rng->range.min, rng->by_val));
	right = OidOutputFunctionCall(outputfunc, PATHMAN_GET_DATUM(rng->range.max, rng->by_val));

	result = psprintf("[%s: %s)", left, right);
	PG_RETURN_CSTRING(result);
}

/*
 * Returns whole range covered by range partitions
 */
Datum
get_whole_range(PG_FUNCTION_ARGS)
{
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;
	RangeEntry		   *ranges;
	PathmanRange	   *rng;
	int					parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));

	prel = get_pathman_relation_info(parent_oid, NULL);
	rangerel = get_pathman_range_relation(parent_oid, NULL);

	if (!prel || !rangerel || prel->parttype != PT_RANGE || rangerel->ranges.elem_count == 0)
		PG_RETURN_NULL();

	ranges = dsm_array_get_pointer(&rangerel->ranges, true);

	/* Create range entry object */
	rng = palloc(sizeof(PathmanRange));
	rng->type_oid = prel->atttype;
	rng->by_val = rangerel->by_val;
	rng->range.child_oid = 0;
	rng->range.min = ranges[0].min;
	rng->range.max = ranges[rangerel->ranges.elem_count-1].max;

	PG_RETURN_POINTER(rng);
}

/*
 * Returns 0 if value fits into range, -1 if it's less than lower bound
 * and 1 if it's greater than upper bound
 */
Datum
range_value_cmp(PG_FUNCTION_ARGS)
{
	TypeCacheEntry *tce;
	PathmanRange   *rng = (PathmanRange *) PG_GETARG_POINTER(0);
	Datum		value = PG_GETARG_DATUM(1);
	Oid			value_type = get_fn_expr_argtype(fcinfo->flinfo, 1);
	Oid			cmp_proc_oid;
	FmgrInfo	cmp_func;
	int 		lower_cmp,
				upper_cmp;

	tce = lookup_type_cache(value_type, TYPECACHE_BTREE_OPFAMILY);

	cmp_proc_oid = get_opfamily_proc(tce->btree_opf,
									 value_type,
									 rng->type_oid,
									 BTORDER_PROC);
	fmgr_info(cmp_proc_oid, &cmp_func);

	/* check lower bound */
	lower_cmp = FunctionCall2(&cmp_func, value, PATHMAN_GET_DATUM(rng->range.min, rng->by_val));
	if (DatumGetInt32(lower_cmp) < 0)
		PG_RETURN_INT32(-1);

	/* check upper bound */
	upper_cmp = FunctionCall2(&cmp_func, value, PATHMAN_GET_DATUM(rng->range.max, rng->by_val));
	if (DatumGetInt32(upper_cmp) >= 0)
		PG_RETURN_INT32(1);

	PG_RETURN_INT32(0);
}

/* Returns range lower bound */
Datum
range_lower(PG_FUNCTION_ARGS)
{
	PathmanRange   *rng = (PathmanRange *) PG_GETARG_POINTER(0);
	Oid				return_type = get_fn_expr_rettype(fcinfo->flinfo);
	Datum			result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* If lower bound value isn't the same as return type then convert it */
	if (rng->type_oid != return_type)
	{
		bool by_val = get_typbyval(return_type);

		result = cast_datum(rng->range.min, rng->type_oid, return_type);
		PG_RETURN_DATUM(PATHMAN_GET_DATUM(result, by_val));
	}
	else
		PG_RETURN_DATUM(PATHMAN_GET_DATUM(rng->range.min, rng->by_val));
}

/* Returns range upper bound */
Datum
range_upper(PG_FUNCTION_ARGS)
{
	PathmanRange   *rng = (PathmanRange *) PG_GETARG_POINTER(0);
	Oid				return_type = get_fn_expr_rettype(fcinfo->flinfo);
	Datum			result;

	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	/* If lower bound value isn't the same as return type then convert it */
	if (rng->type_oid != return_type)
	{
		bool by_val = get_typbyval(return_type);

		result = cast_datum(rng->range.max, rng->type_oid, return_type);
		PG_RETURN_DATUM(PATHMAN_GET_DATUM(result, by_val));
	}
	else
		PG_RETURN_DATUM(PATHMAN_GET_DATUM(rng->range.max, rng->by_val));
}

/* Returns range relation's oid */
Datum
range_oid(PG_FUNCTION_ARGS)
{
	PathmanRange   *rng = (PathmanRange *) PG_GETARG_POINTER(0);

	PG_RETURN_OID(rng->range.child_oid);
}


/*
 * Returns set of table partitions
 */
Datum
range_partitions_list(PG_FUNCTION_ARGS)
{
	int			parent_oid = DatumGetInt32(PG_GETARG_DATUM(0));
	FuncCallContext    *funcctx;
	MemoryContext		oldcontext;
	PathmanRangeListCtxt *fctx;
	PartRelationInfo   *prel;
	RangeRelation	   *rangerel;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
	
		prel = get_pathman_relation_info(parent_oid, NULL);
		rangerel = get_pathman_range_relation(parent_oid, NULL);
		if (!prel || !rangerel || prel->parttype != PT_RANGE || rangerel->ranges.elem_count == 0)
			SRF_RETURN_DONE(funcctx);

		/* Switch context when allocating stuff to be used in later calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		fctx = (PathmanRangeListCtxt *) palloc(sizeof(PathmanRangeListCtxt));
		fctx->pos = 0;
		fctx->ranges = dsm_array_get_pointer(&rangerel->ranges, true);
		fctx->by_val = rangerel->by_val;
		fctx->nranges = prel->children_count;
		fctx->type_oid = prel->atttype;

		/* Return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);

		funcctx->user_fctx = fctx;
	}

	funcctx = SRF_PERCALL_SETUP();
	fctx = funcctx->user_fctx;
	if (fctx->pos < fctx->nranges)
	{
		int pos;
		PathmanRange *rng = (PathmanRange *) palloc(sizeof(PathmanRange));

		pos = fctx->pos;
		fctx->pos++;

		rng->type_oid = fctx->type_oid;
		rng->by_val = fctx->by_val;
		rng->range = fctx->ranges[pos];

		SRF_RETURN_NEXT(funcctx, PointerGetDatum(rng));
	}
	else
		SRF_RETURN_DONE(funcctx);
}

Datum
get_hash(PG_FUNCTION_ARGS)
{
	uint32	value = PG_GETARG_UINT32(0),
			part_count = PG_GETARG_UINT32(1);

	PG_RETURN_UINT32(make_hash(value, part_count));
}
