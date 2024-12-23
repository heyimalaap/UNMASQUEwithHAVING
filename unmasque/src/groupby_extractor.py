import datetime
from typing import Any, List, Tuple

from loguru import logger

from .context import UnmasqueContext

def add_to_value(key_type: str, value, delta):
    if key_type == 'date':
        return value + datetime.timedelta(days=delta)

    if key_type == 'integer' or key_type == 'numeric':  # INT, NUMERIC
        return value + delta

    # Otherwise, for string we simply return an arbritary dummy string
    if value == 'a':
        return 'b'
    return 'a'

def groupby_extractor(ctx: UnmasqueContext):
    def has_two_rows() -> bool:
        res, _ = ctx.connection.sql(ctx.hidden_query)
        return len(res) == 2

    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def add_duplicate_rows_new_vals(table: str, attrib: str, value: Any):
        if type(value) is not int:
            value = f"'{value}'"
        ctx.connection.sql(f'CREATE TABLE {table}_tmp AS SELECT * FROM {table};', execute_only=True)
        ctx.connection.sql(f'UPDATE {table}_tmp SET {attrib}={value};', execute_only=True)
        ctx.connection.sql(f'INSERT INTO {table} SELECT * FROM {table}_tmp;', execute_only=True)
        ctx.connection.sql(f'DROP TABLE {table}_tmp;', execute_only=True)

    def get_attrib_value(table: str, attrib: str):
        res, _ = ctx.connection.sql(f'SELECT DISTINCT({attrib}) FROM {table};', fetch_one=True)
        return res[0]

    def is_groupby_attrib_with_val(table: str, attrib: str, val: Any) -> bool:
        begin_transaction()
        add_duplicate_rows_new_vals(table, attrib, val)
        for join in ctx.join_graph:
            if (table, attrib) in join:
                for table_attrib in join:
                    if table_attrib != (table, attrib):
                        add_duplicate_rows_new_vals(table_attrib[0], table_attrib[1], val)
        is_empty = has_two_rows()
        rollback()
        return is_empty

    if ctx.core_relations is None:
        raise RuntimeError('Cannot run without metadata extraction and from clause extraction')

    logger.info('Starting Group By extractor')
    groupby_attribs: List[Tuple[str, str]] = []
    skip_check_attribs: List[Tuple[str, str]] = []

    for table in ctx.core_relations:
        groupby_candidate_attribs = ctx.minimized_attributes[table]

        for attrib in groupby_candidate_attribs:
            if (table, attrib) in skip_check_attribs:
                continue

            val = get_attrib_value(table, attrib)
            val_plus_1 = add_to_value(ctx.db_attribs_types[table][attrib], val, 1)
            val_minus_1 = add_to_value(ctx.db_attribs_types[table][attrib], val, -1)
            logger.debug(f'Checking groupby for {(table, attrib)} with original val {val} (delta+ = {val_plus_1}, delta- = {val_minus_1})')
            if is_groupby_attrib_with_val(table, attrib, val_plus_1) or is_groupby_attrib_with_val(table, attrib, val_minus_1):
                groupby_attribs.append((table, attrib))
                for join in ctx.join_graph:
                    if (table, attrib) in join:
                        skip_check_attribs.extend(join)

    ctx.set_groupby_extractor(groupby_attribs)
    logger.debug(f'Extracted group by attributes: {groupby_attribs}')

    logger.info('Finished Group By extractor')
