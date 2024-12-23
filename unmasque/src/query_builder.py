from .context import UnmasqueContext

def query_from_context(ctx: UnmasqueContext):
    if ctx.core_relations is None:
        return None

    core_relation_list = ", ".join([str(r) for r in ctx.core_relations])

    projection_list = ", ".join([str(r) if aggr is None else f"{aggr}({str(r)})" for aggr, r in zip(ctx.projection_aggregations, ctx.projected_attrib)])

    predicate = []
    tables = set()
    for join in ctx.join_graph:
        predicate.extend([f'{a[0]}.{a[1]} = {b[0]}.{b[1]}' for a, b in zip(join, join[1:])])
        for table, _ in join:
            tables.add(table)

    for table, attrib, bound, val in ctx.filter_predicates:
        predicate.append(f'{table}.{attrib} {bound} {val}')

    query = f'SELECT {projection_list} FROM {core_relation_list}'
    if predicate:
        query += f'\n\tWHERE {" AND ".join(predicate)}'
    if ctx.groupby_attribs:
        groupby_list = [f'{table}.{attrib}' for table, attrib in ctx.groupby_attribs]
        query += f'\n\tGROUP BY {", ".join(groupby_list)}'
    if ctx.having_predicates:
        having_list = [f'{agg}({table}.{attrib}) {bound} {val}' for table, attrib, agg, bound, val in ctx.having_predicates]
        query += f'\n\tHAVING {" AND ".join(having_list)}'
    if ctx.has_orderby:
        query += f'\n\tORDER BY {ctx.orderby_string}'

    query += ';'
    return query
