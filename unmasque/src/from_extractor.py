from loguru import logger
from .context import UnmasqueContext

def from_extractor(ctx: UnmasqueContext):
    logger.info('Starting From Clause Extractor')
    def relation_is_core_relation(table_name: str):
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)
        ctx.connection.sql(f'ALTER TABLE {table_name} RENAME TO {table_name}_tmp;', execute_only=True)
        ctx.connection.sql(f'CREATE TABLE {table_name} (LIKE {table_name}_tmp);', execute_only=True)
        res, _ = ctx.connection.sql(ctx.hidden_query)
        ctx.connection.sql('ROLLBACK TRANSACTION;', execute_only=True)

        return len(res) == 0

    core_relations = []

    if ctx.db_relations is None:
        logger.error('Cannot proceed without having information about tables in the database')
        raise RuntimeError('Cannot proceed without having information about tables in the database')

    for table in ctx.db_relations:
        if relation_is_core_relation(table):
            core_relations.append(table)

    logger.debug(f'Extracted core relations {core_relations}')
    ctx.set_from_extractor(core_relations)

    logger.info('Finishing From Clause Extractor')

