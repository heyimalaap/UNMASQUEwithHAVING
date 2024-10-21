from typing import Dict, List, Tuple
from loguru import logger
from .context import UnmasqueContext

DEBUG_MINIMIZER = True

def minimizer(ctx: UnmasqueContext):
    table_attributes_map: Dict[str, list[str]] = dict()
    minimized_attributes: Dict[str, List[str]] = dict()

    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def commit_transaction():
        ctx.connection.sql('COMMIT;', execute_only=True)

    def rollback_transaction():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def empty_qurey_result() -> bool:
        res, _ = ctx.connection.sql(ctx.hidden_query)
        return len(res) == 0

    def remove_all_rows_except_with_value(table, attribute, value):
        if type(value) is not int:
            value = f"'{value}'"
        ctx.connection.sql(f"DELETE FROM {table} WHERE {attribute} != {value};", execute_only=True)
        pass

    def get_freq_value_of_attrib(table: str, attribute: str):
        q = f"SELECT {attribute}, COUNT(*) FROM {table} GROUP BY {attribute};"
        res, _ = ctx.connection.sql(q)
        return res

    def from_catalog_where():
        return f" FROM information_schema.columns WHERE table_schema = '{ctx.connection.schema}' and TABLE_CATALOG= '{ctx.connection.db_name}' "

    def get_attributes(table_name: str) -> list[str]:
        """Queries the database for the attributes of a table
        
        Args:
            table_name (str): Name of the table whoes attributes we want
        """

        # Cache results to reduce database overhead
        if table_attributes_map.get(table_name):
            return table_attributes_map[table_name]

        res, _ = ctx.connection.sql(f"SELECT column_name {from_catalog_where()} and table_name = '{table_name}';")
        table_attributes_map[table_name] = [row[0] for row in res]
        
        return table_attributes_map[table_name]

    def get_frequency_of_values(table_name: str, except_in: List[str] = []) -> Dict[Tuple[str, str | int], int]:
        """Returns a dictionary where the key is a tuple of the form (Attribute, Value) and the value is the frequency"""

        freq: Dict[Tuple[str, str | int], int] = dict()

        for attr in get_attributes(table_name):
            if attr in except_in:
                continue # we ignore this attribute

            freq_vals = get_freq_value_of_attrib(table, attr)
            for fv in freq_vals:
                v, f = fv[0], fv[1]
                freq[(attr, v)] = f

        return freq

    def get_frequency_sorted_attr_value(table_name: str, except_in: List[str] = []) -> Tuple[List[Tuple[str, str | int]], Dict[Tuple[str, str | int], int]]:
        """Returns a list of (Attribute, Value) tuples that are sorted in the order of frequency"""

        freq = get_frequency_of_values(table_name, except_in)
        result = list(freq.keys())
        result.sort(key=lambda x: freq[x], reverse=True)

        return result, freq

    def dbg_log(log: str):
        if DEBUG_MINIMIZER:
            logger.debug(log)

    logger.info('Starting minimizer')

    if ctx.core_relations is None:
        raise RuntimeError('Cannot run minimizer without extracting metadata.')
    
    is_minimized = False
    minimized: Dict[str, List[str]] = dict()
    for table in ctx.core_relations:
        minimized[table] = []

    while not is_minimized:
        for table in ctx.core_relations:
            is_minimized = True
            dbg_log(f"[+] Minimizing table {table}")
            sorted_attrib_val, _ = get_frequency_sorted_attr_value(table, minimized[table])


            for attrib, value in sorted_attrib_val:
                dbg_log(f'\t[*] Trying {attrib} = {value}')
                begin_transaction()
                remove_all_rows_except_with_value(table, attrib, value)

                # if the result was empty, then roll back!
                if empty_qurey_result():
                    rollback_transaction()
                    continue
                
                is_minimized = False
                minimized[table].append(attrib)
                commit_transaction()
                break

    for table in ctx.core_relations:
        ctids, _ = ctx.connection.sql(f'SELECT ctid FROM {table};')
        for ctid in ctids:
            begin_transaction()
            ctx.connection.sql(f'DELETE FROM {table} WHERE ctid=\'{ctid[0]}\';', execute_only=True)
            if empty_qurey_result():
                rollback_transaction()
                continue
            commit_transaction()

    minimized_attributes = minimized

    ctx.set_minimizer(table_attributes_map, minimized_attributes)

    logger.info('Finishing minimizer')
