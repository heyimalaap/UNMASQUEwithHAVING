from .context import UnmasqueContext
from loguru import logger
import copy
import frozenlist
import datetime
from decimal import Decimal

# Constants
NO_ORDER = 'noorder'
COUNT = 'COUNT'
SUM = 'SUM'
ORPHAN_COLUMN = "'?column?'"
NUMBER_TYPES = ['int', 'integer', 'number', 'numeric', 'float', 'decimal', 'Decimal']
NON_TEXT_TYPES = ['date'] + NUMBER_TYPES

dummy_int = 2
dummy_char = 65  # to avoid having space/tab
dummy_date = datetime.date(1000, 1, 1)
dummy_varbit = format(0, "b")
dummy_boolean = True

def get_char(dchar):
    try:
        chr(dchar)
    except TypeError:
        return dchar
    return chr(dchar)

def get_int(dchar):
    try:
        ord(dchar)
    except TypeError:
        return dchar
    return ord(dchar)

def get_dummy_val_for(datatype):
    if datatype in NUMBER_TYPES:
        return dummy_int
    elif datatype == 'date':
        return dummy_date
    else:
        return dummy_char

def get_val_plus_delta(datatype, min_val, delta):
    if min_val is None:
        return None
    plus_delta = min_val
    try:
        if datatype == 'date':
            plus_delta = min_val + datetime.timedelta(days=delta)
        elif datatype == 'int':
            plus_delta = min_val + delta
        elif datatype == 'numeric':
            plus_delta = float(Decimal(min_val) + Decimal(delta))
        elif datatype == 'char':
            plus_delta = get_int(min_val) + delta
        return plus_delta
    except OverflowError:
        return min_val

def get_unused_dummy_val(datatype, value_used):
    global dummy_int, dummy_date, dummy_char
    if datatype in NUMBER_TYPES:
        dint = dummy_int
    elif datatype == 'date':
        dint = dummy_date
    elif datatype in ['char', 'str']:
        if dummy_char == 91:
            dummy_char = 65
        dint = get_char(dummy_char)
    else:
        raise ValueError

    while dint in value_used:
        dint = get_val_plus_delta(datatype, dint, 1)

    if datatype in NUMBER_TYPES:
        dummy_int = dint
    elif datatype == 'date':
        dummy_date = dint
    elif datatype in ['char', 'str']:
        dint = get_char(dint)
        dummy_char = get_int(dint)
    else:
        raise ValueError
    return dint

def get_format(datatype, val):
    if datatype in ['date', 'char', 'character', 'character varying', 'str', 'text', 'varchar']:
        return f'\'{str(val)}\''
    elif datatype in ['numeric', 'float', 'decimal', 'Decimal']:
        val = float(val)
        return str(round(val, 3))
    return str(val)


class CandidateAttribute:
    def __init__(self, logger, attrib, aggregation, dependency, dependencyList, attrib_dependency, index, name):
        self.attrib = attrib
        self.aggregation = aggregation
        self.dependency = dependency
        self.dependencyList = copy.deepcopy(dependencyList)
        self.attrib_dependency = copy.deepcopy(attrib_dependency)
        self.index = index
        self.name = name

    def debug_print(self):
        logger.debug(f"attrib: {self.attrib}")
        logger.debug(f"aggregation: {self.aggregation}")
        logger.debug(f"dependency: {self.dependency}")
        logger.debug(f"dependency list: {self.dependencyList}")
        logger.debug(f"attrib_dependency: {self.attrib_dependency}")
        logger.debug(f"projection index: {self.index}")
        logger.debug('')

def tryConvert(unused, val):
    changed = False
    try:
        temp = int(val)
        changed = True
    except Exception as e:
        temp = val
    if not changed:
        try:
            temp = float(val)
            changed = True
        except Exception as e:
            temp = val
    return temp

def check_sort_order(logger, lst):
    if all(tryConvert(logger, lst[i]) <= tryConvert(logger, lst[i + 1]) for i in range(len(lst) - 1)):
        return "asc"
    elif all(tryConvert(logger, lst[i]) >= tryConvert(logger, lst[i + 1]) for i in range(len(lst) - 1)):
        return "desc"
    else:
        return NO_ORDER

def fmt(val):
    val_fmt = val
    if (type(val) is not int) and (type(val) is not Decimal) and (type(val) is not float):
        val_fmt = f"'{val}'"
    return str(val_fmt)

class OrderBy:
    def __init__(self, ctx: UnmasqueContext):
        self.values_used = []
        self.global_projection_names = ctx.projection_names
        self.projected_attribs = ctx.projected_attrib
        self.global_aggregated_attributes = []
        for proj, aggr in zip(ctx.projected_attrib, ctx.projection_aggregations):
            if aggr is not None:
                self.global_aggregated_attributes.append((proj, aggr))
            else:
                self.global_aggregated_attributes.append((proj, ''))
        self.orderby_list = []
        self.global_dependencies = ctx.projection_deps
        self.joined_attribs = []
        for _, attrib in ctx.projection_joined_attribs:
            self.joined_attribs.append(attrib)
        self.orderBy_string = ''
        self.has_orderBy = True
        self.joined_attrib_valDict = {}
        self.ctx = ctx
        self.filter_attrib_dict = ctx.filter_attrib_dict
        self.dmin = dict()
        # Hack: Populate dmin
        for table in ctx.core_relations:
            self.dmin[table] = dict()
            for attrib in ctx.table_attributes_map[table]:
                val, _ = ctx.connection.sql(f"SELECT {attrib} FROM {table};", fetch_one=True)
                val = val[0]
                self.dmin[table][attrib] = float(val) if isinstance(val, Decimal) else val

    def begin_transaction(self):
        self.ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback(self):
        self.ctx.connection.sql('ROLLBACK;', execute_only=True)


    def run(self):
        query = self.ctx.hidden_query
        # ORDERBY ON PROJECTED COLUMNS ONLY
        # ASSUMING NO ORDER ON JOIN ATTRIBUTES
        cand_list = self.construct_candidate_list()
        logger.debug("candidate list: ", cand_list)
        # CHECK ORDER BY ON COUNT
        self.orderBy_string = self.get_order_by(cand_list, query)
        self.has_orderBy = True if len(self.orderby_list) else False
        logger.debug("order by string: ", self.orderBy_string)
        logger.debug("order by list: ", self.orderby_list)
        
        self.ctx.set_orderby_extraction(self.has_orderBy, self.orderby_list, self.orderBy_string)
        return True
    
    def get_dmin_value(self, table: str, attrib: str):
        return self.dmin[table][attrib]

    def truncate_core_relations(self):
        if self.ctx.core_relations is None:
            return
        for table in self.ctx.core_relations:
            self.ctx.connection.sql(f"Truncate Table {table};", execute_only=True)

    def insert_attrib_vals_into_table(self, att_order, attrib_list_inner, insert_rows, tabname_inner):
        for row in insert_rows:
            q = f"INSERT INTO {tabname_inner} {att_order} VALUES ({", ".join([fmt(x) for x in row])});"
            self.ctx.connection.sql(q, execute_only=True)

    def get_order_by(self, cand_list, query):
        # REMOVE ELEMENTS WITH EQUALITY FILTER PREDICATES
        remove_list = []
        for elt in cand_list:
            for entry in self.ctx.filter_predicates:
                if elt.attrib == entry[1] and (entry[2] == '=' or entry[2] == 'equal') and not (
                        SUM in elt.aggregation or COUNT in elt.aggregation):
                    remove_list.append(elt)
        for elt in remove_list:
            cand_list.remove(elt)
        curr_orderby = []
        while self.has_orderBy and cand_list:
            remove_list = []
            self.has_orderBy = False
            row_num = 2
            for elt in cand_list:
                if COUNT in elt.aggregation:
                    row_num = 3
            for elt in cand_list:
                order = self.generateData(elt, self.orderby_list, query, row_num)
                if order is None or elt.name == ORPHAN_COLUMN:
                    remove_list.append(elt)
                elif order != NO_ORDER:
                    self.has_orderBy = True
                    self.orderby_list.append((elt, order))
                    curr_orderby.append(f"{elt.name} {order}")
                    remove_list.append(elt)
                    break
            for elt in remove_list:
                cand_list.remove(elt)
        curr_orderby_str = ", ".join(curr_orderby)

        logger.debug(curr_orderby_str)
        return curr_orderby_str

    def construct_candidate_list(self):
        cand_list = []
        for i in range(len(self.global_aggregated_attributes)):
            dependencyList = []
            for j in range(len(self.global_aggregated_attributes)):
                if j != i and self.global_aggregated_attributes[i][0] == \
                        self.global_aggregated_attributes[j][0]:
                    dependencyList.append((self.global_aggregated_attributes[j]))
            cand_list.append(CandidateAttribute(None, self.global_aggregated_attributes[i][0],
                                                self.global_aggregated_attributes[i][1], not (not dependencyList),
                                                dependencyList, self.global_dependencies[i], i,
                                                self.global_projection_names[i]))
        return cand_list

    def is_part_of_output(self, tab, attrib):
        is_output = False
        for edge in self.ctx.join_graph:
            if (tab, attrib) in edge:
                for o_tab, o_attrib in edge:
                    for agg in self.global_dependencies:
                        if (o_tab, o_attrib) in agg:
                            is_output = True
                            break
        for agg in self.global_dependencies:
            if (tab, attrib) in agg:
                is_output = True
                break
        return is_output

    def get_datatype(self, table_attrib):
        table, attrib = table_attrib
        attrib_type = self.ctx.db_attribs_types[table][attrib]
        if attrib_type in ['int', 'integer', 'number']:
            return 'int'
        elif attrib_type == 'date':
            return 'date'
        elif attrib_type in ['text', 'char', 'varbit', 'varchar2','varchar', 'character', 'character varying']:
            return 'str'
        elif attrib_type in ['numeric', 'float', 'decimal', 'Decimal']:
            return 'numeric'
        else:
            logger.debug(f"Value error on {attrib_type}")
            raise ValueError
        

    def generateData(self, obj, orderby_list, query, row_num):
        # check if it is a key attribute, #NO CHECKING ON KEY ATTRIBUTES
        logger.debug(obj.attrib)
        key_elt = None
        if obj.attrib in self.ctx.projection_joined_attribs:
            for elt in self.ctx.join_graph:
                for item in elt:
                    if obj.attrib in item:
                        key_elt = elt

        if not obj.dependency:
            # ATTRIBUTES TO GET SAME VALUE FOR BOTH ROWS
            # EASY AS KEY ATTRIBUTES ARE NOT THERE IN ORDER AS PER ASSUMPTION SO FAR
            # IN CASE OF COUNT ---
            # Fill 3 rows in any one table (with a a b values) and 2 in all others (with a b values) in D1
            # Fill 3 rows in any one table (with a b b values) and 2 in all others (with a b values) in D2
            same_value_list = []
            for elt in orderby_list:
                for i in elt[0].attrib_dependency:
                    key_f = None
                    for j in self.ctx.join_graph:
                        j_dash = [k[1] for k in j]
                        if i[1] in j_dash:
                            key_f = j
                    # self.logger.debug("Key: ", key_f)
                    if key_f:
                        for in_e in key_f:
                            same_value_list.append(("check", in_e))
                    else:
                        same_value_list.append(i)
            no_of_db = 2
            order = [None, None]

            if self.ctx.core_relations is None:
                return

            # For this attribute (obj.attrib), fill all tables now
            for k in range(no_of_db):
                self.begin_transaction()
                self.truncate_core_relations()
                self.values_used.clear()
                for tabname_inner in self.ctx.core_relations:
                    attrib_list_inner = self.ctx.table_attributes_map[tabname_inner]
                    insert_rows, insert_values1, insert_values2 = [], [], []
                    attrib_list_str = ",".join(attrib_list_inner)
                    att_order = f"({attrib_list_str})"
                    for attrib_inner in attrib_list_inner:
                        datatype = self.get_datatype((tabname_inner, attrib_inner))
                        if self.is_part_of_output(tabname_inner, attrib_inner):
                            if datatype in NON_TEXT_TYPES:
                                first, second = self.get_non_text_attrib(datatype, attrib_inner, tabname_inner)
                            else:
                                first, second = self.get_text_value(attrib_inner, tabname_inner)
                        else:
                            first = self.get_dmin_value(tabname_inner, attrib_inner)
                            second = get_val_plus_delta(datatype, first,
                                                        1) if attrib_inner in self.joined_attribs else first  # first  # get_val_plus_delta(datatype, first,
                            #            1) if attrib_inner in self.joined_attribs else first
                        insert_values1.append(first)
                        insert_values2.append(second)
                        if k == no_of_db - 1 and (any([(attrib_inner in i) for i in
                                                       obj.attrib_dependency]) or 'Count' in obj.aggregation) or (
                                k == no_of_db - 1 and key_elt and attrib_inner in key_elt):  # \
                            # or (k == no_of_db - 1 and key_elt and attrib_inner in key_elt):
                            # swap first and second
                            insert_values2[-1], insert_values1[-1] = insert_values1[-1], insert_values2[-1]

                        if any([(attrib_inner in i) for i in same_value_list]):
                            insert_values2[-1] = insert_values1[-1]
                    if row_num == 3:
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values2))
                    else:
                        insert_rows.append(tuple(insert_values1))
                        insert_rows.append(tuple(insert_values2))

                    self.insert_attrib_vals_into_table(att_order, attrib_list_inner, insert_rows, tabname_inner)

                new_result, _ = self.ctx.connection.sql(self.ctx.hidden_query)
                self.joined_attrib_valDict.clear()
                logger.debug("New Result", k, new_result)
                if len(new_result) == 0:
                    logger.error('some error in generating new database. '
                                      'Result is empty. Can not identify Ordering')
                    return None
                if len(new_result) == 1:
                    return None
                data = new_result # self.app.get_all_nullfree_rows(new_result)
                check_res = []
                for d in data:
                    check_res.append(d[obj.index])
                order[k] = check_sort_order(None, check_res)
                # order[k] = checkOrdering(self.logger, obj, new_result)
                logger.debug("Order", k, order)
                self.rollback()

            if order[0] is not None and order[1] is not None and order[0] == order[1]:
                logger.debug("Order Found", order[0])
                return order[0]
            else:
                return NO_ORDER
        return NO_ORDER

    def get_text_value(self, attrib_inner, tabname_inner):
        # if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
        #     # EQUAL FILTER WILL NOT COME HERE
        #     s_val_text = self.get_s_val_for_textType(attrib_inner, tabname_inner)
        #     if '_' in s_val_text:
        #         string = copy.deepcopy(s_val_text)
        #         first = string.replace('_', get_char(get_dummy_val_for('char')))
        #         string = copy.deepcopy(s_val_text)
        #         second = string.replace('_', get_char(
        #             get_val_plus_delta('char', get_dummy_val_for('char'), 1)))
        #     else:
        #         string = copy.deepcopy(s_val_text)
        #         first = string.replace('%', get_char(get_dummy_val_for('char')), 1)
        #         string = copy.deepcopy(s_val_text)
        #         second = string.replace('%', get_char(
        #             get_val_plus_delta('char', get_dummy_val_for('char'), 1)), 1)
        #     first = first.replace('%', '')
        #     second = second.replace('%', '')
        # else:
        #     first = get_char(get_dummy_val_for('char'))
        #     second = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), 1))
        first = get_char(get_dummy_val_for('char'))
        second = get_char(get_val_plus_delta('char', get_dummy_val_for('char'), 1))
        return first, second

    def get_non_text_attrib(self, datatype, attrib_inner, tabname_inner):
        for edge in self.ctx.join_graph:
            if attrib_inner in edge:
                edge_key = frozenlist.FrozenList(edge)
                edge_key.freeze()
                if edge_key in self.joined_attrib_valDict.keys():
                    return self.joined_attrib_valDict[edge_key][0], self.joined_attrib_valDict[edge_key][1]
        # check for filter (#MORE PRECISION CAN BE ADDED FOR NUMERIC#)
        if datatype != 'date':
            datatype = 'int'
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            first = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            second = min(get_val_plus_delta(datatype, first, 1),
                         self.filter_attrib_dict[(tabname_inner, attrib_inner)][1])
        else:
            first = get_unused_dummy_val(datatype, self.values_used)
            second = get_val_plus_delta(datatype, first, 1)
        self.values_used.extend([first, second])
        for edge in self.ctx.join_graph:
            if attrib_inner in edge:
                edge_key = frozenlist.FrozenList(edge)
                edge_key.freeze()
                if edge_key not in self.joined_attrib_valDict.keys():
                    if datatype == 'date':
                        first = get_format('date', first)
                        second = get_format('date', second)
                    self.joined_attrib_valDict[edge_key] = [first, second]
        return first, second

    def get_date_values(self, attrib_inner, tabname_inner):
        if (tabname_inner, attrib_inner) in self.filter_attrib_dict.keys():
            first = self.filter_attrib_dict[(tabname_inner, attrib_inner)][0]
            second = min(get_val_plus_delta('date', first, 1),
                         self.filter_attrib_dict[(tabname_inner, attrib_inner)][1])
        else:
            first = get_dummy_val_for('date')
            second = get_val_plus_delta('date', first, 1)
        first = get_format('date', first)
        second = get_format('date', second)
        return first, second

def orderby_extractor(ctx: UnmasqueContext):
    logger.info('Starting OrderBy extractor')

    runner = OrderBy(ctx)
    runner.run()

    logger.info('Finished OrderBy extractor')
    
