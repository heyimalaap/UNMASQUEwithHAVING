from datetime import date
import decimal
import math
from loguru import logger
from unmasque2.src.context import UnmasqueContext


def forbidden_set(o1, o2):
    forbidden_alpha_vals = [0, o1, o2, o1 - 1, o2 - 1]
    if o1 != 0:
        forbidden_alpha_vals.append((o1 - o2) / o1)
    if o1 != 1:
        forbidden_alpha_vals.append((1 - o2) / (o1 - 1))
    if (o1 - 2) ** 2 - (4 * (1 - o2)) >= 0:
        forbidden_alpha_vals.append(((o1 - 2) + decimal.Decimal(math.sqrt((o1 - 2) ** 2 - (4 * (1 - o2))))) / 2)

    return forbidden_alpha_vals

def get_aggr_fn(alpha, o1, o2, proj_val):
    r_pv = round(proj_val, 2)

    r_sum = round(alpha * o1 + o2, 2)
    r_avg = round((alpha * o1 + o2) / (alpha + 1), 2)
    r_min = round(min(o1, o2), 2)
    r_max = round(max(o1, o2), 2)
    r_count = alpha + 1

    if r_pv == r_sum:
        return 'SUM'

    if r_pv == r_avg:
        return 'AVG'

    if r_pv == r_min:
        return 'MIN'

    if r_pv == r_max:
        return 'MAX'

    if r_pv == r_count:
        return 'COUNT'

    return ''

def aggregation_extractor(ctx: UnmasqueContext):
    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def get_d1_val(table: str, attrib: str):
        v, _ = ctx.connection.sql(f'SELECT {attrib} FROM {table};', fetch_one=True)
        return v[0]

    def check_predicates(table: str, attrib: str):
        l = None
        u = None
        has_sum = False
        for i in ctx.filter_predicates:
            # eg elem: ('orders', 'o_orderdate', '<=', datetime.date(1995, 10, 13))
            i_table, i_attrib, i_op, i_val = i
            if i_table == table and i_attrib == attrib:
                if i_op == '<=':
                    u = i_val

                if i_op == '>=':
                    l = i_val
            
        for i in ctx.having_predicates:
            # eg elem: ('lineitem', 'l_quantity', 'MIN/Filter', '<=', Decimal('123.00'))
            i_table, i_attrib, i_aggr, i_op, i_val = i
            if i_table == table and i_attrib == attrib:
                if i_op == '<=':
                    u = i_val

                if i_op == '>=':
                    l = i_val

                if i_aggr == 'SUM':
                    has_sum = True

        return has_sum, l, u

    def sum_pred_attribs_on_table(table: str):
        pred_attribs = []
        for i in ctx.having_predicates:
            i_table, i_attrib, i_aggr, _, i_val = i
            if i_table == table and i_attrib not in pred_attribs and i_aggr == 'SUM':
                pred_attribs.append((i_attrib, i_val))
        return pred_attribs

    def fmt(val):
        val_fmt = val
        if (type(val) is not int) and (type(val) is not decimal.Decimal) and (type(val) is not float):
            val_fmt = f"'{val}'"
        return val_fmt

    def init_t1_t2_temp_tabs(table: str):
        uninit_t1_t2_temp_tabs(table)
        ctx.connection.sql(f"CREATE TABLE {table}_t1 (LIKE {table});", execute_only=True)
        ctx.connection.sql(f"CREATE TABLE {table}_t2 (LIKE {table});", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table}_t1 (SELECT * FROM {table} LIMIT 1);", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table}_t2 (SELECT * FROM {table} LIMIT 1);", execute_only=True)

        if ctx.core_relations is None:
            raise Exception("Core relations was None. Run From clause extraction before executing this.")

        for other_table in ctx.core_relations:
            if other_table == table:
                continue

            other_table_sum_attribs = sum_pred_attribs_on_table(other_table)
            for sum_attrib, _ in other_table_sum_attribs:
                ctx.connection.sql(f"ALTER TABLE {other_table} ALTER COLUMN {sum_attrib} TYPE numeric;", execute_only=True)

    def uninit_t1_t2_temp_tabs(table: str):
        ctx.connection.sql(f"DROP TABLE IF EXISTS {table}_t1;", execute_only=True)
        ctx.connection.sql(f"DROP TABLE IF EXISTS {table}_t2;", execute_only=True)
    
    def gen_t1(table: str, attrib: str, s1):
        sum_attribs = sum_pred_attribs_on_table(table)
        ctx.connection.sql(f"UPDATE {table}_t1 SET {attrib} = {fmt(s1)};", execute_only=True)
        for s_attrib, _ in sum_attribs:
            ctx.connection.sql(f"UPDATE {table}_t1 SET {s_attrib} = {fmt(-1)};", execute_only=True)

        o1_query = query_QJ(table, "t1")
        res, _ = ctx.connection.sql(o1_query, fetch_one=True)
        return res

    def gen_t2(table: str, attrib: str, s2, alpha: int):
        sum_attribs = sum_pred_attribs_on_table(table)
        ctx.connection.sql(f"UPDATE {table}_t2 SET {attrib} = {fmt(s2)};", execute_only=True)
        for s_attrib, s_val in sum_attribs:
            ctx.connection.sql(f"UPDATE {table}_t2 SET {s_attrib} = {fmt(s_val + alpha)};", execute_only=True)

        o2_query = query_QJ(table, "t2")
        res, _ = ctx.connection.sql(o2_query, fetch_one=True)
        return res

    def query_QJ(table: str, mod: str):
        if ctx.core_relations is None:
            return ''
        core_relation_list = ", ".join([str(r) + "_" + mod if str(r) == table else str(r) for r in ctx.core_relations])
        projection_list = ", ".join([str(r) for r in ctx.projected_attrib])
        predicate = []
        for join in ctx.join_graph:
            rename_join = [(a[0] + f"_{mod}" if a[0] == table else a[0], a[1]) for a in join]
            predicate.extend([f'{a[0]}.{a[1]} = {b[0]}.{b[1]}' for a, b in zip(rename_join, rename_join[1:])])
        query = f'SELECT {projection_list} FROM {core_relation_list}'
        if predicate:
            query += f'\n\tWHERE {" AND ".join(predicate)}'

        query += ";"
        return query

    def gen_table(table: str, attrib: str, s1, s2, alpha: int):
        if ctx.core_relations is None:
            raise Exception("Core relations was None. Run From clause extraction before executing this.")

        for other_table in ctx.core_relations:
            if other_table == table:
                continue

            other_table_sum_attribs = sum_pred_attribs_on_table(other_table)
            for sum_attrib, s_val in other_table_sum_attribs:
                update_val = s_val / (alpha + 1)
                ctx.connection.sql(f"UPDATE {other_table} SET {sum_attrib} = {fmt(update_val)};", execute_only=True)

        ctx.connection.sql(f"DELETE FROM {table};", execute_only=True)
        o1_res = gen_t1(table, attrib, s1)
        o2_res = gen_t2(table, attrib, s2, alpha)
        for _ in range(alpha):
            ctx.connection.sql(f"INSERT INTO {table} (SELECT * FROM {table}_t1 LIMIT 1);", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table} (SELECT * FROM {table}_t2 LIMIT 1);", execute_only=True)
        res, _ = ctx.connection.sql(ctx.hidden_query)
        return o1_res, o2_res, res[0], len(res)

    logger.info('Starting Aggregation extractor')

    projection_aggregations = []

    hq_result, _ = ctx.connection.sql(ctx.hidden_query, fetch_one=True)
    for i, pn_and_pd in enumerate(zip(ctx.projection_names, ctx.projection_deps)):
        proj_name, proj_deps = pn_and_pd
        proj_val = hq_result[i]
        logger.info(f'i: {i}, proj_name: {proj_name}, proj_deps: {proj_deps}, proj_val: {proj_val}')

        found = False
        aggr = None

        if len(proj_deps) == 1 and proj_deps[0][1] == proj_name:
            # Most likely a groupby element
            # TODO: come up with a better way
            logger.debug(f"Group by element detected.")
            found = True
            projection_aggregations.append(aggr)
            continue

        for dep in proj_deps:
            d_table, d_attrib = dep
            d_val = get_d1_val(d_table, d_attrib)
            d_has_sum, d_l, d_u = check_predicates(d_table, d_attrib)
            logger.debug(f"\t- {d_table} {d_attrib} : {d_val} {d_has_sum} {d_l} {d_u}")

            # String type cannot have aggregations on them
            if type(d_val) is str:
                found = True
                break

            # TODO: We ignore aggregations on dates for now
            if type(d_val) is date:
                found = True
                break

            if d_l is None and d_u is None:
                s1 = 1
                s2 = 100

                # o1 = get_o_val(d_table, d_attrib, i, s1)
                # o2 = get_o_val(d_table, d_attrib, i, s2)

                # k, agg_map = get_k_value_for_number(o1, o2)
                # logger.debug(f"\t\t + k: {k}, s1: {s1}, s2: {s2}, o1: {o1}, o2: {o2}, agg_map: {agg_map}")

                # aggr = test_for_aggr(d_table, d_attrib, i, s1, s2)
                alpha = 3
                o1 = None
                o2 = None
                proj_val = None
                aggr = None
                while not found:
                    begin_transaction()
                    init_t1_t2_temp_tabs(d_table)
                    logger.info(f"\t\t - Trying alpha = {alpha}")
                    o1_res, o2_res, res, res_len = gen_table(d_table, d_attrib, s1, s2, alpha)
                    if res_len != 1:
                        logger.debug(f"Table length was {res_len}. There is no aggregation on this projection.")
                        aggr = None
                        found = True
                        break
                    o1 = o1_res[i]
                    o2 = o2_res[i]
                    proj_val = res[i]
                    forbidden = forbidden_set(o1, o2)
                    if alpha not in forbidden:
                        found = True
                        aggr = get_aggr_fn(alpha, o1, o2, proj_val)
                        logger.debug(f"\t\t o1 = {o1}, o2 = {o2}, res = {proj_val}")
                        logger.debug(f"\t\t aggr = {aggr}")
                        break
                    alpha = (alpha + 1) * 2 - 1;
                    uninit_t1_t2_temp_tabs(d_table)
                    rollback()

                break
            else:
                if not d_has_sum:
                    s1 = d_l if d_l is not None else d_u - 100
                    s2 = d_u if d_u is not None else d_l + 100
                    logger.debug(f'\t\t  s1 = {s1}, s2 = {s2}')

                    # o1 = get_o_val(d_table, d_attrib, i, s1)
                    # o2 = get_o_val(d_table, d_attrib, i, s2)

                    # k, agg_map = get_k_value_for_number(o1, o2)
                    # logger.debug(f"\t\t + k: {k}, s1: {s1}, s2: {s2}, o1: {o1}, o2: {o2}, agg_map: {agg_map}")

                    # aggr = test_for_aggr(d_table, d_attrib, i, s1, s2)

            
                    alpha = 3
                    o1 = None
                    o2 = None
                    proj_val = None
                    while not found:
                        begin_transaction()
                        init_t1_t2_temp_tabs(d_table)
                        logger.info(f"\t\t - Trying alpha = {alpha}")
                        o1_res, o2_res, res, res_len = gen_table(d_table, d_attrib, s1, s2, alpha)
                        if res_len != 1:
                            logger.debug(f"Table length was {res_len}. There is no aggregation on this projection.")
                            aggr = ''
                            found = True
                            break
                        o1 = o1_res[i]
                        o2 = o2_res[i]
                        proj_val = res[i]
                        forbidden = forbidden_set(o1, o2)
                        if alpha not in forbidden:
                            found = True
                            aggr = get_aggr_fn(alpha, o1, o2, proj_val)
                            logger.debug(f"\t\t o1 = {o1}, o2 = {o2}, res = {proj_val}")
                            logger.debug(f"\t\t aggr = {aggr}")
                            break
                        alpha = (alpha + 1) * 2 - 1;
                        uninit_t1_t2_temp_tabs(d_table)
                        rollback()
                    break
                else:
                    # SUM CASE
                    logger.debug(f'\t\t - We cannot handle this case yet')

        if aggr:
            logger.info(f"Found {aggr}({ctx.projected_attrib[i]})")
        else:
            logger.info(f"Did not find any aggregation")

        projection_aggregations.append(aggr)

    ctx.set_aggregation_extraction(projection_aggregations)
    logger.info('Finished Aggregation extractor')
