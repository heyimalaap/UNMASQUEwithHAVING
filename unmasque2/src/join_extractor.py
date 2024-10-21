import copy
import datetime
from typing import List, Tuple
from loguru import logger
from .context import UnmasqueContext

# Constants
DUMMY_INTS = [2, 3]
DUMMY_CHARS = ['a', 'b']
DUMMY_DATES = [datetime.date(1000, 1, 1), datetime.date(1000, 1, 2)]

def generate_partition_indicies(size):
    res = [[0]]
    for i in range(1, size):
        new_add = []
        for elt in res:
            temp = copy.deepcopy(elt)
            temp.append(i)
            new_add.append(temp)
        for elt in new_add:
            if len(elt) < size:
                res.append(copy.deepcopy(elt))
    return copy.deepcopy(res)


def generate_partition_indicies_for_all_sizes(max_size):
    result = {0: [], 1: []}
    for i in range(2, max_size + 1):
        result[i] = generate_partition_indicies(i)
    return result

def make_partition(key_list, partition_indicies):
    key_list1 = [key_list[i] for i in partition_indicies]
    key_list2 = list(set(key_list) - set(key_list1))
    return key_list1, key_list2

def get_pair_vals(key_type: str):
    if key_type == 'date':
        return DUMMY_DATES

    if key_type == 'integer' or key_type == 'numeric':  # INT, NUMERIC
        return DUMMY_INTS

    # Otherwise
    return DUMMY_CHARS

def join_extractor(ctx: UnmasqueContext):
    def get_type(key_list: List[Tuple[str, str]]) -> str:
        key = key_list[0]
        return ctx.db_attribs_types[key[0]][key[1]]

    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def is_result_empty() -> bool:
        res, _ = ctx.connection.sql(ctx.hidden_query)
        return len(res) == 0

    def assign_value(key_list, value):
        if type(value) is not int:
            value = f"'{value}'"

        for table_attrib in key_list:
            table = table_attrib[0]
            attrib = table_attrib[1]
            ctx.connection.sql(f"UPDATE {table} SET {attrib} = {value};", execute_only=True)

    if ctx.core_relations is None:
        raise RuntimeError('Cannot run without metadata extraction and from clause extraction')

    logger.info('Starting Join extractor')

    key_lists = copy.deepcopy(ctx.key_lists)
    join_graph = []

    max_size_clique = max(len(elt) for elt in key_lists)
    partition_combos = generate_partition_indicies_for_all_sizes(max_size_clique)

    while key_lists:
        key_list = key_lists.pop()

        # We only care about relations that are in the from clause
        key_list = [key for key in key_list if key[0] in ctx.core_relations]

        if len(key_list) <= 1:
            continue
        logger.debug(f'Checking join condition in the clique {key_list}')

        partitoned = False
        for partition_indicies in partition_combos[len(key_list)]:
            key_list1, key_list2 = make_partition(key_list, partition_indicies)
            key_type = get_type(key_list)
            logger.debug(f'key_list1: {key_list1}, key_list2: {key_list2}, type: {key_type}')
            val1, val2 = get_pair_vals(key_type)

            begin_transaction()
            assign_value(key_list1, val1)
            assign_value(key_list2, val2)
            result_empty = is_result_empty()
            rollback()

            if not result_empty:
                # The edges joining key_list1 and key_list2 are not in the join
                # graph, thus, we put the partitioned cycles back into key_lists
                key_lists.append(key_list1)
                key_lists.append(key_list2)
                partitoned = True
                break

        if not partitoned:
            join_graph.append(key_list)

    logger.debug(f'Final join graph: {join_graph}')
    ctx.set_join_extractor(join_graph)
    logger.info('Finished Join extractor')
