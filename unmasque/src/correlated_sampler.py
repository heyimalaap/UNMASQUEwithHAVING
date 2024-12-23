import copy
from loguru import logger
from .context import UnmasqueContext

MAX_SAMPLING_ATTEMPTS = 100
SAMPLE_SIZE_MULTIPLIER = 10
INITIAL_SAMPLE_SIZE_PERCENT = 0.16

DEBUG_SAMPLER = True

def correlated_sampler(ctx: UnmasqueContext) -> bool:
    if ctx.core_relations is None:
        raise RuntimeError("Cannot do sampling without extraction of metadata")

    def empty_qurey_result() -> bool:
        res, _ = ctx.connection.sql(ctx.hidden_query)
        return len(res) == 0

    def get_base_t(key_list, sizes):
        max_cs = 0
        base_t = 0
        for i in range(0, len(key_list)):
            if max_cs < sizes[key_list[i][0]]:
                max_cs = sizes[key_list[i][0]]
                base_t = i
        return base_t

    def do_for_key_lists(sample_size_percent, sizes):
        if ctx.core_relations is None:
            raise RuntimeError("Cannot do sampling without extraction of metadata")

        for key_list in ctx.key_lists:
            base_t = get_base_t(key_list, sizes)

            # Sample base table
            base_table = key_list[base_t][0]
            base_key = key_list[base_t][1]
            if base_table in ctx.core_relations:
                limit_row = sizes[base_table]
                ctx.connection.sql(f"""
                                   INSERT INTO {base_table} SELECT * FROM {base_table}_restore 
                                                            TABLESAMPLE SYSTEM({sample_size_percent}) 
                                                            WHERE ({base_key}) NOT IN (SELECT DISTINCT({base_key}) FROM {base_table}) LIMIT {limit_row};
                                   """, execute_only=True)
                if DEBUG_SAMPLER:
                    res, _ = ctx.connection.sql(f'SELECT COUNT(*) FROM {base_table};', fetch_one=True)
                    logger.debug(f'Sampled {res[0]} items from {base_table}')

            # sample remaining tables from key_list using the sampled base table
            for i in range(0, len(key_list)):
                tabname2 = key_list[i][0]
                key2 = key_list[i][1]

                # if tabname2 in not_sampled_tables:
                if tabname2 != base_table and tabname2 in ctx.core_relations:
                    limit_row = sizes[tabname2]
                    ctx.connection.sql(f"""
                                       INSERT INTO {tabname2} SELECT * FROM {tabname2}_restore WHERE {key2} IN (SELECT DISTINCT({base_key}) FROM {base_table}) AND {key2} NOT IN (SELECT DISTINCT({key2}) FROM {tabname2}) LIMIT {limit_row};
                                       """,execute_only=True)
                    if DEBUG_SAMPLER:
                        res, _ = ctx.connection.sql(f'SELECT COUNT(*) FROM {tabname2};', fetch_one=True)
                        logger.debug(f'Sampled {res[0]} items from {tabname2}')

    def do_for_empty_key_lists(sample_size_percent, not_sampled_tables):
        if len(ctx.key_lists) == 0:
            for table in not_sampled_tables:
                ctx.connection.sql(f"INSERT INTO {table} SELECT * FROM {table}_restore TABLESAMPLE SYSTEM({sample_size_percent});", execute_only=True)
                if DEBUG_SAMPLER:
                    res, _ = ctx.connection.sql(f'SELECT COUNT(*) FROM {table};', fetch_one=True)
                    logger.debug(f'Sampled {res[0]} items from {table}')

    def sample(sample_size_percent: float, sizes) -> bool:
        if ctx.core_relations is None:
            raise RuntimeError("Cannot do sampling without extraction of metadata")

        do_for_key_lists(sample_size_percent, sizes)
        not_sampled_tables = copy.deepcopy(ctx.core_relations)
        do_for_empty_key_lists(sample_size_percent, not_sampled_tables)

        if empty_qurey_result():
            return False

        # Update post sampled sizes
        for table in ctx.core_relations:
            res, _ = ctx.connection.sql(f'SELECT COUNT(*) FROM {table};', fetch_one=True)
            size = res[0]

            ctx.db_relation_sizes[table] = size

        return True

    logger.info("Starting Correlated Sampler.")

    sample_size_percent = INITIAL_SAMPLE_SIZE_PERCENT 
    attempts_left = MAX_SAMPLING_ATTEMPTS
    while attempts_left > 0:
        logger.info(f'Starting sampling attempt {MAX_SAMPLING_ATTEMPTS - attempts_left}')
        success = sample(sample_size_percent, ctx.db_relation_sizes)
        if not success:
            logger.warning(f'Sampling attempt {MAX_SAMPLING_ATTEMPTS - attempts_left} failed.')
            sample_size_percent *= SAMPLE_SIZE_MULTIPLIER
            attempts_left -= 1
        else:
            logger.info("Finishing Correlated Sampler.")
            return True

    logger.error(f'Failed correlated sampling completely. Instead using the whole DB. All subsequent stages may be slow.')
    for table in ctx.core_relations:
        ctx.connection.sql(f'INSERT INTO {table} SELECT * FROM {table}_restore;',execute_only=True)
    return False


