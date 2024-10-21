import csv
from typing import Dict, List
from loguru import logger
from .context import UnmasqueContext

# TODO: Deal with this
SCHEMA_PATH = './pkfkrelations.csv'

def get_tables(ctx: UnmasqueContext) -> List[str]:
    return ctx.connection.table_names()

# TODO: Clean this up
def get_pk_fk_graph(db_tables: List[str]):
    all_pkfk = []
    with open(SCHEMA_PATH, 'rt') as f:
        data = csv.reader(f)
        all_pkfk = list(data)[1:]

    pk_dict = dict()
    key_lists = [[]]
    
    temp = []
    for row in all_pkfk:
        if row[2].upper() == 'Y':
            pk_dict[row[0]] = pk_dict.get(row[0], '') + ("," if row[0] in temp else '') + \
                                          row[1]
            temp.append(row[0])
        found_flag = False
        for elt in key_lists:
            if (row[0], row[1]) in elt or (row[4], row[5]) in elt:
                if (row[0], row[1]) not in elt and row[1]:
                    elt.append((row[0], row[1]))
                if (row[4], row[5]) not in elt and row[4]:
                    elt.append((row[4], row[5]))
                found_flag = True
                break
        if found_flag:
            continue
        if row[0] and row[4]:
            key_lists.append([(row[0], row[1]), (row[4], row[5])])
        elif row[0]:
            key_lists.append([(row[0], row[1])])


    key_lists = [list(filter(lambda val: val[0] in db_tables, elt)) for elt in
                             key_lists if elt and len(elt) > 1]

    return pk_dict, key_lists

def get_attrib_types_and_maxlen(ctx: UnmasqueContext):
    if ctx.core_relations is None:
        return {}

    db_attribs_types: Dict[str, Dict[str, str]] = dict()
    db_attribs_max_length: Dict[str, Dict[str, int]] = dict()
    for table in ctx.core_relations:
        res, _ = ctx.connection.sql(f"SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_schema='{ctx.connection.schema}' AND table_name='{table}';")
        for row in res:
            attrib = row[0]
            attrib_type = row[1]
            attrib_max_length = int(str(row[2])) if row[2] is not None else 0
            
            if db_attribs_types.get(table) is None:
                db_attribs_types[table] = dict()

            if db_attribs_max_length.get(table) is None:
                db_attribs_max_length[table] = dict()

            db_attribs_types[table][attrib] = attrib_type
            db_attribs_max_length[table][attrib] = attrib_max_length

    return db_attribs_types, db_attribs_max_length
    

def metadata_extractor_stage1(ctx: UnmasqueContext):
    logger.info('Starting Metadata extractor stage 1')

    logger.debug('Fetching tables in the database')
    db_tables = get_tables(ctx)

    ctx.set_metadata1(db_tables)

    logger.info('Finishing Metadata extractor stage 2')

def metadata_extractor_stage2(ctx: UnmasqueContext):
    logger.info('Starting Metadata extractor stage 2')

    if ctx.core_relations is None or ctx.db_relations is None:
        raise RuntimeError('Cannot run stage 2 minimizer without running from clause extractor')

    logger.debug('Fetching sizes of tables in the database')
    db_table_sizes = dict()
    for table in ctx.db_relations:
        res, _ = ctx.connection.sql(f"SELECT COUNT(*) FROM {table};", fetch_one=True)
        db_table_sizes[table] = res[0]

    logger.debug('Extracting primary keys and building PK-FK closure list from schema file')
    pk_dict, key_lists = get_pk_fk_graph(ctx.db_relations)

    db_attribs_types, db_attribs_max_length = get_attrib_types_and_maxlen(ctx)

    ctx.set_metadata2(db_table_sizes, pk_dict, key_lists, db_attribs_types, db_attribs_max_length)

    logger.info('Finishing Metadata extractor stage 2')
