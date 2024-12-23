import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any
import math

from loguru import logger

from .projection_extractor import get_min_and_max_val
from .context import UnmasqueContext

# Constants
# These magic numbers are from https://www.postgresql.org/docs/8.1/datatype.htm
MIN_DATE_VALUE = datetime.date(1, 1, 1)
MAX_DATE_VALUE = datetime.date(9999, 12, 31)
MIN_INT_VALUE = -2147483648
MAX_INT_VALUE = 2147483647
MIN_NUMERIC_VALUE = Decimal('-2147483648')
MAX_NUMERIC_VALUE = Decimal('2147483647')

def one(datatype: str):
    if datatype == 'date':
        return datetime.timedelta(days=-1), datetime.timedelta(days=1)
    return -1, 1 

def predicate_extractor(ctx: UnmasqueContext):

    # Utils
    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def commit():
        ctx.connection.sql('COMMIT;', execute_only=True)

    def is_result_empty() -> bool:
        res, _ = ctx.connection.sql(ctx.hidden_query)
        return len(res) == 0

    def get_attribs(table: str) -> list[str]:
        return ctx.table_attributes_map[table]

    def is_pk(table: str, attrib: str) -> bool:
        table_pks = ctx.pk_dict[table].split(',')
        return attrib in table_pks

    def is_key(table: str, attrib: str) -> bool:
        for key_list in ctx.key_lists:
            if (table, attrib) in key_list:
                return True
        return False

    def get_attrib_type(table: str, attrib: str):
        return ctx.db_attribs_types[table][attrib]

    def get_ctid_attrib_val(table: str, attribute: str, sorted=True) -> list[tuple[str, Any]]:
        res, _ = ctx.connection.sql(f'SELECT ctid, {attribute} FROM {table} ORDER BY {attribute};')
        return res

    def get_attrib_min_max_value(table: str, attrib: str):
        attrib_type = get_attrib_type(table, attrib)

        match attrib_type:
            case "date":
                return MIN_DATE_VALUE, MAX_DATE_VALUE
            case "integer" | "int":
                return MIN_INT_VALUE, MAX_INT_VALUE
            case "numeric":
                # TODO: NUMERIC allows for user defined precision. Thus we need
                # query the database for min bound. For now, let's just use a 
                # constant
                return MIN_NUMERIC_VALUE, MAX_NUMERIC_VALUE
            case _:
                raise RuntimeError('Min/Max value of string type makes no sense at all.') 

    def get_attrib_cur_val(table: str, attrib: str):
        res, _ = ctx.connection.sql(f'SELECT DISTINCT({attrib}) from {table};', fetch_one=True)
        return res[0]

    def is_result_empty_with_attrib_value(table: str, attrib: str, value: Any, ctid = None):
        if type(value) is not int:
            value = f"'{value}'"
        begin_transaction()
        if ctid is None:
            ctx.connection.sql(f'UPDATE {table} SET {attrib} = {value};', execute_only=True)
        else:
            ctx.connection.sql(f'UPDATE {table} SET {attrib} = {value} WHERE ctid = \'{ctid}\';', execute_only=True)

        is_empty = is_result_empty()
        rollback()
        return is_empty

    def binary_search(table: str, attrib: str, low, high, search_side: str, ctid = None):
        attrib_type = get_attrib_type(table, attrib)
        minus_one, plus_one = one(attrib_type)
        min_val, max_val = get_attrib_min_max_value(table, attrib)

        if search_side == 'l':
            def mid_lb(l, h):
                if attrib_type == 'date':
                    return l + datetime.timedelta(days=math.floor((h - l).days / 2))
                else:
                    return math.floor((l + h) / 2)

            # Coarse search
            def coarse_search_lb(low, x):
                if attrib_type != 'date':
                    l = Decimal(low)
                    h = Decimal(x)
                else:
                    l = low
                    h = x

                m = None
                while l < h:
                    m = mid_lb(l, h)
                    if not is_result_empty_with_attrib_value(table, attrib, m, ctid):
                        h = m
                    else:
                        l = min(max_val, m + plus_one)

                if attrib_type != 'date':
                    h = int(h)
                return h


            # Refined search
            def refine_lb(lb, p = 2):
                p = 10 ** p

                l = Decimal(max(lb - 1, min_val))
                h = Decimal(lb)
                p_inv = 1/Decimal(p)

                m = None
                while l < h:
                    m = ((l + h) / 2).quantize(p_inv, rounding=ROUND_FLOOR)
                    if not is_result_empty_with_attrib_value(table, attrib, m, ctid):
                        h = m
                    else:
                        l = min(max_val, m + p_inv)

                return h

            lb = coarse_search_lb(low, high)
            if attrib_type == 'numeric':
                lb = refine_lb(lb)
            return lb

        elif search_side == 'r':
            def mid_ub(l, h):
                if attrib_type == 'date':
                    return l + datetime.timedelta(days=math.ceil((h - l).days / 2))
                else:
                    return math.ceil((l + h) / 2)

            # Coarse search
            def coarse_search_ub(x, high):
                if attrib_type != 'date':
                    l = Decimal(x)
                    h = Decimal(high)
                else:
                    l = x
                    h = high

                m = None
                while l < h:
                    m = mid_ub(l, h)
                    if not is_result_empty_with_attrib_value(table, attrib, m, ctid):
                        l = m
                    else:
                        h = max(min_val, m + minus_one)

                if attrib_type != 'date':
                    l = int(l)
                return l

            # Refined search
            def refine_ub(ub, p = 2):
                p = 10 ** p

                l = Decimal(ub)
                h = Decimal(min(ub + 1, max_val))
                p_inv = 1/Decimal(p)

                m = None
                while l < h:
                    m = ((l + h) / 2).quantize(p_inv, rounding=ROUND_CEILING)
                    if not is_result_empty_with_attrib_value(table, attrib, m, ctid):
                        l = m
                    else:
                        h = m - p_inv

                return l

            ub = coarse_search_ub(low, high)
            if attrib_type == 'numeric':
                ub = refine_ub(ub)
            return ub
        
        return None


    def get_filter_predicate(table: str, attrib: str):
        if get_attrib_type(table, attrib) not in  ['int', 'integer', 'numeric', 'date']:
            # TODO: Implement filter for strings
            return None

        min_val, max_val = get_attrib_min_max_value(table, attrib)
        val = get_attrib_cur_val(table, attrib)
        r1_is_phi = is_result_empty_with_attrib_value(table, attrib, min_val)
        r2_is_phi = is_result_empty_with_attrib_value(table, attrib, max_val)

        if not r1_is_phi and not r2_is_phi:
            return None

        l = None
        r = None
        if r1_is_phi:
            l = binary_search(table, attrib, min_val, val, 'l')

        if r2_is_phi:
            r = binary_search(table, attrib, val, max_val, 'r')

        logger.debug(f'{(table, attrib)}, r1 = {r1_is_phi}, r2 = {r2_is_phi}, l = {l}, r = {r}')
        predicates = []
        if r1_is_phi:
            predicates.append((table, attrib, ">=", l))

        if r2_is_phi:
            predicates.append((table, attrib, "<=", r))

        # TODO: Remove this later
        min_val, max_val = get_attrib_min_max_value(table, attrib)
        filter_attrib_dict[(table, attrib)] = (min_val if l is None else l, max_val if r is None else r)

        return predicates

    def get_lower_bound(table: str, attribute: str) -> Any:
        begin_transaction()
        ctid_vals = get_ctid_attrib_val(table, attribute, sorted=True)
        v = None
        min_val, _ = get_attrib_min_max_value(table, attribute)
        for _, (ctid, val) in enumerate(ctid_vals):
            v = binary_search(table, attribute, min_val, val, 'l', ctid)
            if v == min_val:
                v_fmted = v
                if type(v) is not int:
                    v_fmted = f"'{v}'"
                ctx.connection.sql(f"UPDATE {table} SET {attribute} = {v_fmted} WHERE ctid='{ctid}';", execute_only=True)
                v = None
                continue

            v_fmted = v
            if type(v) is not int:
                v_fmted = f"'{v}'"
            ctx.connection.sql(f"UPDATE {table} SET {attribute} = {v_fmted} WHERE ctid='{ctid}';", execute_only=True)
            break

        commit()
        # rollback()

        # TODO: This is a hack to get decimal numbers working. Do this properly later
        if type(v) is Decimal:
            v = v.quantize(Decimal('0.00'))

        if v is None:
            return None

        # Check for valid bound
        candidate_bounds = [v]

        res, _ = ctx.connection.sql(f'SELECT SUM({table}.{attribute}) FROM {table};', fetch_one=True)
        candidate_bounds.append(res[0])

        res, _ = ctx.connection.sql(f'SELECT AVG({table}.{attribute}) FROM {table};', fetch_one=True)
        candidate_bounds.append(res[0])

        candidate_bounds.sort()

        for lb in candidate_bounds:
            begin_transaction()

            ctx.connection.sql(f'UPDATE {table} SET {attribute}=NULL;', execute_only=True)
            ctid_vals = get_ctid_attrib_val(table, attribute, sorted=True)
            first_ctid, _ = ctid_vals[0]

            lb_fmt = lb
            if type(lb) is not int:
                lb_fmt = f"'{lb}'"

            ctx.connection.sql(f"UPDATE {table} SET {attribute}={lb_fmt} WHERE ctid='{first_ctid}';", execute_only=True)
            was_empty = is_result_empty()

            rollback()

            if not was_empty:
                return lb

        return v

    def get_upper_bound(table: str, attribute: str) -> Any:
        begin_transaction()
        ctid_vals = get_ctid_attrib_val(table, attribute, sorted=True)
        v = None
        _, max_val = get_attrib_min_max_value(table, attribute)
        # for i, (ctid, val) in reversed(list(enumerate(ctid_vals))):
        for i in range(len(ctid_vals) - 1, -1, -1):
            ctid, val = get_ctid_attrib_val(table, attribute, sorted=True)[i]
            v = binary_search(table, attribute, val, max_val, 'r', ctid)
            if v == max_val:
                v_fmted = v
                if type(v) is not int:
                    v_fmted = f"'{v}'"
                ctx.connection.sql(f"UPDATE {table} SET {attribute} = {v_fmted} WHERE ctid='{ctid}';", execute_only=True)
                v = None
                continue

            v_fmted = v
            if type(v) is not int:
                v_fmted = f"'{v}'"
            ctx.connection.sql(f"UPDATE {table} SET {attribute} = {v_fmted} WHERE ctid='{ctid}';", execute_only=True)
            break

        commit()
        # rollback()

        # TODO: This is a hack to get decimal numbers working. Do this properly later
        if type(v) is Decimal:
            v = v.quantize(Decimal('0.00'))

        if v is None:
            return None

        # Check for valid bound
        candidate_bounds = [v]

        res, _ = ctx.connection.sql(f'SELECT SUM({table}.{attribute}) FROM {table};', fetch_one=True)
        candidate_bounds.append(res[0])

        res, _ = ctx.connection.sql(f'SELECT AVG({table}.{attribute}) FROM {table};', fetch_one=True)
        candidate_bounds.append(res[0])

        candidate_bounds.sort(reverse=True)

        for ub in candidate_bounds:
            begin_transaction()

            ctx.connection.sql(f'UPDATE {table} SET {attribute}=NULL;', execute_only=True)
            ctid_vals = get_ctid_attrib_val(table, attribute, sorted=True)
            first_ctid, _ = ctid_vals[0]

            ub_fmt = ub
            if type(ub) is not int:
                ub_fmt = f"'{ub}'"

            ctx.connection.sql(f"UPDATE {table} SET {attribute}={ub_fmt} WHERE ctid='{first_ctid}';", execute_only=True)
            was_empty = is_result_empty()

            rollback()

            if not was_empty:
                return ub

        return v

    def make_deflated_db_instance(predicate_candidates):
        if ctx.core_relations is None:
            raise RuntimeError('Cannot run without from clause extraction')

        begin_transaction()

        for table in ctx.core_relations:
            ctx.connection.sql(f"ALTER TABLE {table} RENAME TO {table}_tmp;", execute_only=True)
            ctx.connection.sql(f"CREATE TABLE {table} (LIKE {table}_tmp);", execute_only=True)
            ctx.connection.sql(f"INSERT INTO {table} (SELECT * FROM {table}_tmp LIMIT 1);", execute_only=True)
            ctx.connection.sql(f"DROP TABLE {table}_tmp;", execute_only=True)

            for p in predicate_candidates:
                p_tabname, p_attrib, p_lb, p_ub = p
                if p_tabname != table:
                    continue

                val = p_lb if p_lb is not None else p_ub
                val_fmt = val
                if type(val) is not int:
                    val_fmt = f"'{val}'"

                ctx.connection.sql(f"UPDATE {p_tabname} set {p_attrib} = {val_fmt};", execute_only=True)

        # Sanity Check
        if is_result_empty():
            logger.error('Sanity check failed. Failed to deflate.')
            rollback()
            raise RuntimeError('Failed to create a deflated database instance.')
        else:
            logger.debug('Sanity check succeeded. Deflate successful.')
            commit()

    def fmt(val):
        fmted = val
        if type(val) is not int:
            fmted = f"'{val}'"
        return fmted

    def check_predicate(predicate_candidates: list, table: str, attrib: str, k1, k2) -> bool:
        table_predicate_candidates = [p for p in predicate_candidates if p[0] == table]
        other_predicates = [p for p in table_predicate_candidates if p[1] != attrib]

        begin_transaction()
        ctx.connection.sql(f"ALTER TABLE {table} RENAME TO {table}_tmpbak;", execute_only=True)
        ctx.connection.sql(f"CREATE TABLE {table} (LIKE {table}_tmpbak);", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table} SELECT * FROM {table}_tmpbak;", execute_only=True)
        commit()

        fmt_k1 = fmt(k1)
        fmt_k2 = fmt(k2)

        begin_transaction()
        ctx.connection.sql(f"UPDATE {table} SET {attrib} = {fmt_k1};", execute_only=True)
        ctx.connection.sql(f"CREATE TABLE {table}_tmp (LIKE {table});", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table}_tmp SELECT * FROM {table};", execute_only=True)
        commit()

        for c in table_predicate_candidates:
            other_attrib = c[1]
            ctx.connection.sql(f"UPDATE {table}_tmp SET {other_attrib} = NULL;", execute_only=True)

        for c in other_predicates:
            other_attrib = c[1]
            fmt_other_sval = fmt(c[2] if c[2] is not None else c[3])

            ctx.connection.sql(f"UPDATE {table}_tmp SET {other_attrib} = {fmt_other_sval};", execute_only=True)
            begin_transaction()
            ctx.connection.sql(f"INSERT into {table} SELECT * FROM {table}_tmp;", execute_only=True)
            flag = is_result_empty()
            rollback()

            if flag:
                ctx.connection.sql(f"UPDATE {table}_tmp SET {other_attrib} = NULL;", execute_only=True)


        begin_transaction()
        ctx.connection.sql(f"UPDATE {table}_tmp SET {attrib} = {fmt_k2};", execute_only=True)
        ctx.connection.sql(f"INSERT into {table} SELECT * FROM {table}_tmp;", execute_only=True)
        was_empty = is_result_empty()
        rollback()


        begin_transaction()
        ctx.connection.sql(f"DROP TABLE {p_tab}_tmp;", execute_only=True)
        ctx.connection.sql(f"DROP TABLE {table};", execute_only=True)
        ctx.connection.sql(f"ALTER TABLE {table}_tmpbak RENAME TO {table};", execute_only=True)
        commit()

        return was_empty

    # Utils end

    logger.info('Starting Predicate extractor')

    if ctx.core_relations is None:
        raise RuntimeError('Cannot perform predicate extraction without from '\
                           'clause extraction')

    having_predicates = []
    filter_predicates = []
    separable_predicates = []
    filter_attrib_dict = dict() # TODO: This is added here to retrofit OrderBy from UNMASQUE's code.

    # If there is no group-by attribute, then this means that the query was an
    # SPJ query, then we do not need to use the HAVING machinary on this query.
    # This is handled below
    if len(ctx.groupby_attribs) == 0:

        for table in ctx.core_relations:
            for attrib in get_attribs(table):
                if not is_pk(table, attrib):
                    predicates = get_filter_predicate(table, attrib)
                    if predicates is not None:
                        filter_predicates.extend(predicates)
        ctx.set_predicate_extractor(filter_predicates, having_predicates)
        logger.info('Finished Predicate extractor')
        return

    # Extract filter predicate on group-by attributes
    for table_attrib in ctx.groupby_attribs:
        table, attrib = table_attrib
        if not is_pk(table, attrib):
            predicates = get_filter_predicate(table, attrib)
            if predicates is not None:
                filter_predicates.extend(predicates)

    # Pass 1
    predicate_candidates = []
    for table in ctx.core_relations:
        for attrib in get_attribs(table):
            # Ignore predicates extraction on string types 
            # TODO: Extract string predicates later
            if get_attrib_type(table, attrib) not in ['int', 'integer', 'date', 'numeric']:
                continue

            # Ignore group by attributes
            if (table, attrib) in ctx.groupby_attribs:
                continue

            # Ignore key attributes (Part of assumptions)
            if is_key(table, attrib):
                continue

            # Dates can only have filter predicate
            if get_attrib_type(table, attrib) == 'date':
                predicates = get_filter_predicate(table, attrib)
                if predicates is not None:
                    filter_predicates.extend(predicates)
                continue

            # Lower bound
            lb = get_lower_bound(table, attrib)

            # Upper bound
            ub = get_upper_bound(table, attrib) 

            min_val, max_val = get_attrib_min_max_value(table, attrib)
            filter_attrib_dict[(table, attrib)] = (min_val if lb is None else lb, max_val if ub is None else ub)

            if lb is not None or ub is not None:
                predicate_candidates.append((table, attrib, lb, ub))

    logger.debug(f'Predicate candidates: {predicate_candidates}')

    # Deflate tables
    make_deflated_db_instance(predicate_candidates)

    # Pass 2
    for pc in predicate_candidates:
        aggr_fn_name = None
        separable = False
        p_tab, p_attrib, p_lb, p_ub = pc

        # Try extracting predicate name from upper bound first
        if p_ub is not None:
            # Test for SUM
            k = p_ub
            was_empty = check_predicate(predicate_candidates, p_tab, p_attrib, k, k)
            if was_empty:
                aggr_fn_name = 'SUM'

            # Test for MAX
            if aggr_fn_name is None:
                k1 = p_ub - 1
                k2 = p_ub + 1
                was_empty = check_predicate(predicate_candidates, p_tab, p_attrib, k1, k2)

                if was_empty:
                    aggr_fn_name = 'MAX'

            # Test for AVG
            if aggr_fn_name is None:
                k1 = p_ub
                k2 = p_ub + 2
                was_empty = check_predicate(predicate_candidates, p_tab, p_attrib, k1, k2)

                if was_empty:
                    aggr_fn_name = 'AVG'

            # Test for MIN
            # TODO: Figure out a way to differentiate between MIN and filter 
            #       predicates
            if aggr_fn_name is None:
                separable = True
                aggr_fn_name = 'MIN/Filter'

        elif p_lb is not None:
            # Test for MIN
            k1 = p_lb - 1
            k2 = p_lb + 1
            was_empty = check_predicate(predicate_candidates, p_tab, p_attrib, k1, k2)
            if was_empty:
                aggr_fn_name = 'MIN'

            # Test for AVG
            if aggr_fn_name is None:
                k1 = p_lb - 3
                k2 = p_lb + 1
                was_empty = check_predicate(predicate_candidates, p_tab, p_attrib, k1, k2)
                if was_empty:
                    aggr_fn_name = 'AVG'

            # Test for MAX
            if aggr_fn_name is None:
                k1 = p_lb - 1
                k2 = 1
                was_empty = check_predicate(predicate_candidates, p_tab, p_attrib, k1, k2)
                if not was_empty:
                    aggr_fn_name = 'SUM'

            # Test for MAX
            # TODO: Figure out a way to differentiate between MAX and filter 
            #       predicates
            if aggr_fn_name is None:
                separable = True
                aggr_fn_name = 'MAX/Filter'
        else:
            raise RuntimeError(f'{pc} has no LB or UB')

        if p_ub:
            having_predicates.append((p_tab, p_attrib, aggr_fn_name, '<=', p_ub))
        if p_lb:
            having_predicates.append((p_tab, p_attrib, aggr_fn_name, '>=', p_lb))

        if separable:
            separable_predicates.append((p_tab, p_attrib, aggr_fn_name, p_lb, p_ub))


    # Set context
    ctx.set_predicate_extractor(filter_predicates, having_predicates, separable_predicates, filter_attrib_dict)

    logger.info('Finished Predicate extractor')

