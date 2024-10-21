from loguru import logger
from .context import UnmasqueContext

def predicate_separator(ctx: UnmasqueContext):
    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def sum_pred_attribs_on_table(table: str):
        pred_attribs = []
        for i in ctx.having_predicates:
            i_table, i_attrib, i_aggr, _, i_val = i
            if i_table == table and i_attrib not in pred_attribs and i_aggr == 'SUM':
                pred_attribs.append((i_attrib, i_val))
        return pred_attribs

    def fmt(val):
        val_fmt = val
        if type(val) is not int:
            val_fmt = f"'{val}'"
        return val_fmt

    def init_t1_t2_temp_tabs(table: str):
        uninit_t1_t2_temp_tabs(table)

        if ctx.core_relations is None:
            raise Exception("Core relations was None. Run From clause extraction before executing this.")

        for other_table in ctx.core_relations:
            other_table_sum_attribs = sum_pred_attribs_on_table(other_table)
            for sum_attrib, sval in other_table_sum_attribs:
                ctx.connection.sql(f"ALTER TABLE {other_table} ALTER COLUMN {sum_attrib} TYPE numeric;", execute_only=True)
                ctx.connection.sql(f"UPDATE {other_table} SET {sum_attrib} = {fmt(sval / 2)};", execute_only=True)

        ctx.connection.sql(f"CREATE TABLE {table}_t1 (LIKE {table});", execute_only=True)
        ctx.connection.sql(f"CREATE TABLE {table}_t2 (LIKE {table});", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table}_t1 (SELECT * FROM {table} LIMIT 1);", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table}_t2 (SELECT * FROM {table} LIMIT 1);", execute_only=True)

    def uninit_t1_t2_temp_tabs(table: str):
        ctx.connection.sql(f"DROP TABLE IF EXISTS {table}_t1;", execute_only=True)
        ctx.connection.sql(f"DROP TABLE IF EXISTS {table}_t2;", execute_only=True)
    
    def gen_t1(table: str, attrib: str, s1):
        ctx.connection.sql(f"UPDATE {table}_t1 SET {attrib} = {fmt(s1)};", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table} (SELECT * FROM {table}_t1);", execute_only=True)

    def gen_t2(table: str, attrib: str, s2):
        ctx.connection.sql(f"UPDATE {table}_t2 SET {attrib} = {fmt(s2)};", execute_only=True)
        ctx.connection.sql(f"INSERT INTO {table} (SELECT * FROM {table}_t2);", execute_only=True)

    def query_QJ():
        if ctx.core_relations is None:
            return ''
        core_relation_list = ", ".join([str(r) for r in ctx.core_relations])
        projection_list = ", ".join([str(r) if aggr is None else f"{aggr}({str(r)})" for aggr, r in zip(ctx.projection_aggregations, ctx.projected_attrib)])
        predicate = []
        for join in ctx.join_graph:
            predicate.extend([f'{a[0]}.{a[1]} = {b[0]}.{b[1]}' for a, b in zip(join, join[1:])])
        query = f'SELECT {projection_list} FROM {core_relation_list}'
        if predicate:
            query += f'\n\tWHERE {" AND ".join(predicate)}'
        if ctx.groupby_attribs:
            groupby_list = [f'{table}.{attrib}' for table, attrib in ctx.groupby_attribs]
            query += f'\n\tGROUP BY {", ".join(groupby_list)}'
        query += ";"
        return query

    logger.info('Starting Predicate separator')
    logger.debug(f"Predicates to separate: {ctx.separatable_predicates}")

    join_only_query = query_QJ()

    having_predicates = [p for p in ctx.having_predicates]
    filter_predicates = [p for p in ctx.filter_predicates]

    for sp in ctx.separatable_predicates:
        sp_table, sp_attrib, sp_aggr, a, b = sp
        begin_transaction()
        init_t1_t2_temp_tabs(sp_table)

        r1 = None
        if sp_aggr == 'MIN/Filter':
            # MIN/Filter(Attrib) <= b
            gen_t1(sp_table, sp_attrib, b)
            r1, _ = ctx.connection.sql(join_only_query, fetch_one=True)
            gen_t2(sp_table, sp_attrib, b+1)

            ground_truth, _ = ctx.connection.sql(ctx.hidden_query, fetch_one=True)
            if r1 == ground_truth:
                sp_aggr = 'Filter'
                new_having_pred = []
                for p in having_predicates:
                    if p[0] == sp_table and p[1] == sp_attrib:
                        continue
                    new_having_pred.append(p)
                new_filter_pred = [p for p in filter_predicates]
                new_filter_pred.append((sp_table, sp_attrib, "<=", b))
                if a is not None:
                    new_filter_pred.append((sp_table, sp_attrib, ">=", a))

                having_predicates = new_having_pred
                filter_predicates = new_filter_pred
            else:
                sp_aggr = 'MIN'
                new_having_pred = []
                for p in having_predicates:
                    if p[0] == sp_table and p[1] == sp_attrib:
                        new_having_pred.append((sp_table, sp_attrib, 'MIN', '<=', b))
                        if a is not None:
                            new_having_pred.append((sp_table, sp_attrib, 'MIN', '>=', a))
                    else:
                        new_having_pred.append(p)
                new_filter_pred = [p for p in filter_predicates]

                having_predicates = new_having_pred
                filter_predicates = new_filter_pred
        elif sp_aggr == 'MAX/Filter':
            # a <= MAX/Filter(Attrib)
            gen_t1(sp_table, sp_attrib, a)
            r1, _ = ctx.connection.sql(join_only_query, fetch_one=True)
            gen_t2(sp_table, sp_attrib, a-1)
            ground_truth, _ = ctx.connection.sql(ctx.hidden_query, fetch_one=True)
            if r1 == ground_truth:
                sp_aggr = 'Filter'
                new_having_pred = []
                for p in having_predicates:
                    if p[0] == sp_table and p[1] == sp_attrib:
                        continue
                    new_having_pred.append(p)
                new_filter_pred = [p for p in filter_predicates]
                new_filter_pred.append((sp_table, sp_attrib, ">=", a))
                if b is not None:
                    new_filter_pred.append((sp_table, sp_attrib, "<=", b))

                having_predicates = new_having_pred
                filter_predicates = new_filter_pred
            else:
                sp_aggr = 'MAX'
                new_having_pred = []
                for p in having_predicates:
                    if p[0] == sp_table and p[1] == sp_attrib:
                        new_having_pred.append((sp_table, sp_attrib, 'MAX', '>=', a))
                        if b is not None:
                            new_having_pred.append((sp_table, sp_attrib, 'MAX', '<=', b))
                    else:
                        new_having_pred.append(p)
                new_filter_pred = [p for p in filter_predicates]

                having_predicates = new_having_pred
                filter_predicates = new_filter_pred
            
        logger.debug(f"Aggregation on {sp_table}.{sp_attrib} is {sp_aggr}")
        uninit_t1_t2_temp_tabs(sp_table)
        rollback()

    ctx.set_predicate_separator(filter_predicates, having_predicates)
    logger.info('Finished Predicate separator')
