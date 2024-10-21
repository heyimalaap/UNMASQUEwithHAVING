from loguru import logger
from decimal import Decimal

from sympy import Integer, Rational, collect, expand, factor, nsimplify, symbols
from .context import UnmasqueContext
import ast
import datetime
import copy
import numpy as np
import random
import math


CONST_1_VALUE = '1'
NUMBER_TYPES = ['int', 'integer', 'number', 'numeric', 'float', 'decimal', 'Decimal']
NON_TEXT_TYPES = ['date'] + NUMBER_TYPES
IDENTICAL_EXPR = "identical_expr_nc"

dummy_int = 2
dummy_char = 65  # to avoid having space/tab
dummy_date = datetime.date(1000, 1, 1)
dummy_varbit = format(0, "b")
dummy_boolean = True
min_numeric_val = -2147483648.88
max_numeric_val = 2147483647.88
min_int_val = -2147483648
max_int_val = 2147483647
min_date_val = datetime.date(1, 1, 1)
max_date_val = datetime.date(9999, 12, 31)
pr_min = 1
pr_max = 999



def get_min_and_max_val(datatype):
    global min_numeric_val, max_numeric_val
    global min_int_val, max_int_val
    global min_date_val, max_date_val

    if datatype == 'date':
        return min_date_val, max_date_val
    elif datatype in ['int', 'integer', 'number']:
        return min_int_val, max_int_val
    elif datatype in ['numeric', 'float', 'decimal', 'Decimal']:
        return min_numeric_val, max_numeric_val
    else:
        return min_int_val, max_int_val

def get_format(datatype, val):
    if datatype in ['date', 'char', 'character', 'character varying', 'str', 'text', 'varchar']:
        return f'\'{str(val)}\''
    elif datatype in ['numeric', 'float', 'decimal', 'Decimal']:
        val = float(val)
        return str(round(val, 3))
    return str(val)

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
    global dummy_int, dummy_char, dummy_date, dummy_varbit, dummy_boolean
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

def get_dummy_val_for(datatype):
    if datatype in NUMBER_TYPES:
        return dummy_int
    elif datatype == 'date':
        return dummy_date
    else:
        return dummy_char

def get_boundary_value(v_cand, is_ub):
    if isinstance(v_cand, list) or isinstance(v_cand, tuple):
        v_val = v_cand[-1] if is_ub else v_cand[0]
        if isinstance(v_val, tuple):
            v_val = v_val[-1] if is_ub else v_val[0]
    else:
        v_val = v_cand
    return v_val


def count_empty_lists_in(l):
    return sum(x.count([]) for x in l)

def if_dependencies_found_incomplete(projection_names, projection_dep):
    if len(projection_names) > 2:
        empty_deps = count_empty_lists_in(projection_dep)
        if len(projection_dep) - empty_deps < 2:
            return True
    return False

def get_subsets(deps):
    res = []
    get_subsets_helper(deps, res, [], 0)
    return res


def get_subsets_helper(deps, res, curr, idx):
    res.append(curr[:])
    for i in range(idx, len(deps)):
        curr.append(deps[i])
        get_subsets_helper(deps, res, curr, i + 1)
        curr.pop()

def get_param_values_external(coeff_arr):
    # print(coeff_arr)
    subsets = get_subsets(coeff_arr)
    subsets = sorted(subsets, key=len)
    # print(subsets)
    final_lis = []
    for i in subsets:
        temp_val = 1
        if len(i) < 2 and i == []:
            continue
        for j in range(len(i)):
            temp_val *= i[j]
        final_lis.append(temp_val)
    return final_lis

def round_expr(expr, num_digits):
    return expr.xreplace({n: round(n, num_digits) for n in expr.atoms(Rational) if not isinstance(n, Integer)})

def projection_extractor(ctx: UnmasqueContext):
    # Utils
    def begin_transaction():
        ctx.connection.sql('BEGIN TRANSACTION;', execute_only=True)

    def rollback():
        ctx.connection.sql('ROLLBACK;', execute_only=True)

    def get_attribs(table: str) -> list[str]:
        return ctx.table_attributes_map[table]

    def get_projection_column_type(projection_names):
        query = ctx.hidden_query
        mod_query = query.replace(";", '')
        datatype_names = []
        for i in projection_names:
            typecheck_query = str(
                'SELECT pg_typeof(' + i + ') AS data_type FROM (' + mod_query + ') AS subquery limit 1;')
            type_result, _ = ctx.connection.sql(typecheck_query)
            modified_str = str(type_result[0])
            modified_str = modified_str[1:-2]
            datatype_names.append(modified_str)
        datatype_names = [element.strip("'") for element in datatype_names]
        return datatype_names

    def get_other_attribs_in_eqjoin_grp(tabname, attrib):
        other_attribs = []
        for join_edge in ctx.join_graph:
            if tabname is not None:
                if (tabname, attrib) in join_edge:
                    other_attribs = copy.deepcopy(join_edge)
                    other_attribs.remove((tabname, attrib))
                    break
            else:
                for item in join_edge:
                    if item[1] == attrib:
                        other_attribs = copy.deepcopy(join_edge)
                        other_attribs.remove((tabname, attrib))
                        break
        return other_attribs

    def get_datatype(attrib_type: str) -> str:
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

    def get_attrib_type(table: str, attrib: str):
        return ctx.db_attribs_types[table][attrib]

    def get_predicate_attribs():
        predicate_attribs = []
        for item in ctx.filter_predicates:
            predicate_attribs.append((item[0], item[1]))
        for item in ctx.having_predicates:
            predicate_attribs.append((item[0], item[1]))
        return predicate_attribs

    def get_datatype_of_attrib(tabname: str, attrib: str):
        attrib_type = get_attrib_type(tabname, attrib)
        datatype = get_datatype(attrib_type)
        return datatype

    def get_type_lb_ub_filter_attrib(tabname: str, attrib: str):
        attrib_type = get_attrib_type(tabname, attrib)
        datatype = get_datatype(attrib_type)

        lb = None
        ub = None

        for p in ctx.filter_predicates:
            p_tab, p_attrib, p_bound, p_val = p
            if p_tab == tabname and p_attrib == attrib:
                if p_bound == "<=":
                    ub = p_val
                elif p_bound == ">=":
                    lb = p_val

        for p in ctx.having_predicates:
            p_tab, p_attrib, _, p_bound, p_val = p
            if p_tab == tabname and p_attrib == attrib:
                if p_bound == "<=":
                    ub = p_val
                elif p_bound == ">=":
                    lb = p_val

        if lb is None and ub is None:
            logger.info("Cannot generate a new s-val. Giving the old one!")
            return datatype, None, None

        minv, maxv = get_min_and_max_val(datatype)
        if lb is None:
            lb = minv

        if ub is None:
            ub = maxv

        return datatype, lb, ub


    def get_other_than_dmin_val_nonText(attrib: str, tabname: str, prev):
        attrib_type = get_attrib_type(tabname, attrib)
        datatype = get_datatype(attrib_type)
        predicate_attribs = get_predicate_attribs()
        if (tabname, attrib) not in predicate_attribs:
            return get_dummy_val_for(datatype)

        lb = None
        ub = None

        for p in ctx.filter_predicates:
            p_tab, p_attrib, p_bound, p_val = p
            if p_tab == tabname and p_attrib == attrib:
                if p_bound == "<=":
                    ub = p_val
                elif p_bound == ">=":
                    lb = p_val

        for p in ctx.having_predicates:
            p_tab, p_attrib, _, p_bound, p_val = p
            if p_tab == tabname and p_attrib == attrib:
                if p_bound == "<=":
                    ub = p_val
                elif p_bound == ">=":
                    lb = p_val

        if lb is None and ub is None:
            logger.info("Cannot generate a new s-val. Giving the old one!")
            return prev

        minv, maxv = get_min_and_max_val(datatype)
        if lb is None:
            lb = minv

        if ub is None:
            ub = maxv

        lb = get_boundary_value(lb, is_ub=False)
        ub = get_boundary_value(ub, is_ub=True)
        val = ub if prev == lb else lb
        return val

    def get_s_val_for_textType(attrib_inner, tabname_inner) -> str:
        return "b"

    def get_different_s_val(attrib, tabname, prev):
        datatype = get_datatype(get_attrib_type(tabname, attrib))
        logger.debug(f"datatype of {attrib} is {datatype}")
        predicate_attribs = get_predicate_attribs()
        if datatype in NON_TEXT_TYPES:
            if (tabname, attrib) in predicate_attribs:
                val = get_other_than_dmin_val_nonText(attrib, tabname, prev)
            else:
                val = get_unused_dummy_val(datatype, [prev])
            if datatype == 'date':
                val = ast.literal_eval(get_format(datatype, val))
        else:
            if (tabname, attrib) in predicate_attribs:
                val = get_s_val_for_textType(attrib, tabname)
                val = val.replace('%', '')
            else:
                val = get_char(get_unused_dummy_val('char', [prev]))
        return val

    def update_attrib_to_see_impact(attrib, tabname):
        prev, _ = ctx.connection.sql(f"SELECT {attrib} from {tabname};", fetch_one=True)
        prev = prev[0]

        val = get_different_s_val(attrib, tabname, prev)
        logger.debug(f"update {tabname}.{attrib} with value {val} that had previous value {prev}")
        update_with_val(attrib, tabname, val)
        return val, prev

    def update_attribs_bulk(join_tabnames, other_attribs, val):
        join_tabnames.clear()
        for other_attrib in other_attribs:
            join_tabname, join_attrib = other_attrib
            join_tabnames.append(join_tabname)
            update_with_val(join_attrib, join_tabname, val)


    def update_with_val(attrib, tabname, val):
        if val == 'NULL':
            ctx.connection.sql(f"UPDATE {tabname} SET {attrib} = NULL;", execute_only=True)
        else:
            val_fmted = val
            if type(val) is not int:
                val_fmted = f"'{val}'"
            ctx.connection.sql(f"UPDATE {tabname} SET {attrib} = {val_fmted};", execute_only=True)

    def find_diff_idx(list1, list2):
        diffs = []
        for sub_list1, sub_list2 in zip(list1, list2):
            for i, (item1, item2) in enumerate(zip(sub_list1, sub_list2)):
                if item1 != item2 and i not in diffs:
                    diffs.append(i)
        return diffs

    def get_index_of_difference(attrib, old_result, new_result, projection_dep, tabname):
        diff = find_diff_idx(new_result, old_result)
        if diff:
            for d in diff:
                if (tabname, attrib) not in projection_dep[d]:
                    projection_dep[d].append((tabname, attrib))
        return projection_dep

    def check_impact_of_bulk_attribs(attrib, result, projection_dep, tabname, keys_to_skip, s_value_dict):
        if attrib in keys_to_skip:
            return s_value_dict[attrib], keys_to_skip
        join_tabnames = []
        other_attribs = get_other_attribs_in_eqjoin_grp(tabname, attrib)
        val, prev = update_attrib_to_see_impact(attrib, tabname)
        update_attribs_bulk(join_tabnames, other_attribs, val)
        # self.see_d_min()
        new_result, _ = ctx.connection.sql(ctx.hidden_query)
        update_with_val(attrib, tabname, prev)
        update_attribs_bulk(join_tabnames, other_attribs, prev)

        if len(new_result) != 0:
            projection_dep = get_index_of_difference(attrib, result, new_result, projection_dep, tabname)
        keys_to_skip = keys_to_skip + other_attribs
        for other_attrib in other_attribs:
            s_value_dict[other_attrib] = val
        return val, keys_to_skip


    def check_impact_of_single_attrib(attrib, result, projection_dep, tabname, joined_attribs):
        for fe in ctx.filter_predicates:
            if fe[0] == tabname and fe[1] == attrib and (fe[2] == 'equal' or fe[2] == '=') and (fe[0], fe[1]) not in joined_attribs:
                return
        val, prev = update_attrib_to_see_impact(attrib, tabname)
        if val == prev:
            logger.debug("Could not find other s-value! Cannot verify impact!")
            return
        new_result, _ = ctx.connection.sql(ctx.hidden_query)
        update_with_val(attrib, tabname, prev)
        if len(new_result) != 0:
            projection_dep = get_index_of_difference(attrib, result, new_result, projection_dep, tabname)
        else:
            logger.debug("Got empty result!!!!")
        return val

        
    def find_projection_deps(query, s_values):
        # Get column names
        projection_names = []
        result, desc = ctx.connection.sql(ctx.hidden_query)
        for col in desc:
            projection_names.append(col.name)

        # Get column types
        projection_types = get_projection_column_type(projection_names)

        s_values = []
        projected_attrib = []
        keys_to_skip = []
        projection_dep = [[] for _ in projection_names]
        s_value_dict = {}

        joined_attribs = []
        for join in ctx.join_graph:
            for item in join:
                joined_attribs.append(item)

        if ctx.core_relations is None:
            raise RuntimeError('Cannot run projection extraction without running from clause extractor')

        for tabname in ctx.core_relations:
            for attrib in get_attribs(tabname):
                if (tabname, attrib) in joined_attribs:
                    val, keys_to_skip = check_impact_of_bulk_attribs(attrib, result, projection_dep, tabname, keys_to_skip, s_value_dict)
                else:
                    val = check_impact_of_single_attrib(attrib, result, projection_dep, tabname, joined_attribs)
                s_values.append((tabname, attrib, val))

        for i in range(len(projection_names)):
            if len(projection_dep[i]) == 1:
                attrib_tup = projection_dep[i][0]
                projected_attrib.append(attrib_tup[1])
            else:
                if len(projection_dep[i]) == 0 and result[0][i] != CONST_1_VALUE:
                    # If no dependency is there and value is not 1 in result this means it is constant.
                    projected_attrib.append(get_format(projection_types[i], result[0][i]))
                else:
                    # No dependency and value is one so in groupby file will differentiate between count or 1.
                    projected_attrib.append('')

        return projected_attrib, projection_names, projection_dep, True

    g_syms = []
    g_param_list = []

    def construct_value_used_with_dmin():
        # used_val = [val for data in self.global_min_instance_dict.values() for pair in zip(*data) for val in pair]
        used_val = []
        if ctx.core_relations is None:
            return used_val

        for table in ctx.core_relations:
            result, desc = ctx.connection.sql(f"SELECT * FROM {table};", fetch_one=True)
            for i in range(len(result)):
                used_val.append(desc[i].name)
                used_val.append(result[i])
        return used_val

    def get_param_list(deps):
        subsets = get_subsets(deps)
        subsets = sorted(subsets, key=len)
        final_lis = []
        for i in subsets:
            if len(i) < 2 and i == []:
                continue
            temp_str = ""
            for j in range(len(i)):
                temp_str += i[j]
                if j != len(i) - 1:
                    temp_str += "*"
            final_lis.append(temp_str)
        return final_lis

    def __infinite_loop(coeff, dep):
        n = len(dep)
        curr_rank = 1
        outer_idx = 1
        predicate_attribs = get_predicate_attribs()
        while outer_idx < 2 ** n and curr_rank < 2 ** n:
            prev_idx, prev_rank = outer_idx, curr_rank
            # Same algorithm as above with insertion of random values
            # Additionally checking if rank of the matrix has become 2^n
            for j, ele in enumerate(dep):
                key = (ele[0], ele[1])
                mini = pr_min
                maxi = pr_max
                if key in predicate_attribs:
                    datatype, lb, ub = get_type_lb_ub_filter_attrib(key[0], key[1])
                    mini = get_boundary_value(lb, is_ub=False)
                    maxi = get_boundary_value(ub, is_ub=True)
                    if datatype == 'int':
                        coeff[outer_idx][j] = random.randrange(mini, maxi)
                    elif datatype == 'numeric':
                        coeff[outer_idx][j] = round(random.uniform(float(mini), float(maxi)), 2)
                else:
                    coeff[outer_idx][j] = random.randrange(math.floor(mini), math.ceil(maxi))

            temp_array = get_param_values_external(coeff[outer_idx][:n])
            for j in range(2 ** n - 1):
                coeff[outer_idx][j] = temp_array[j]
            coeff[outer_idx][2 ** n - 1] = 1.0
            m_rank = np.linalg.matrix_rank(coeff)
            if m_rank > curr_rank:
                curr_rank += 1
                outer_idx += 1
            if prev_idx == outer_idx and curr_rank == prev_rank:
                logger.debug(coeff)
                logger.debug(dep)
                logger.debug("It will go to infinite loop!! so breaking...")
                break

    def get_attribs(table: str) -> list[str]:
        return ctx.table_attributes_map[table]

    def find_table(attrib: str):
        # This is a hack
        if ctx.core_relations is None:
            return ""

        for table in ctx.core_relations:
            if attrib in get_attribs(table):
                return table

    def update_attrib_in_table(attrib, value, tabname):
        ctx.connection.sql(f"UPDATE {tabname} SET {attrib} = {value};", execute_only=True)

    def get_solution(projected_attrib, projection_dep, idx, value_used, query):
        dep = projection_dep[idx]
        n = len(dep)
        sym_string = ''
        for i in dep:
            sym_string += (i[1] + " ")
        res = 1
        if n > 1:
            syms = symbols(sym_string)
            local_symbol_list = syms
            # syms = sorted(syms, key=lambda x: str(x))
            logger.debug(f"symbols {syms}")
            for i in syms:
                res *= (1 + i)
            logger.debug(f"Sym List {expand(res).args}")
            g_syms.append(get_param_values_external(syms))
            logger.debug(f"Another List {g_syms}")
        else:
            g_syms.append([symbols(sym_string)])
            local_symbol_list = g_syms[-1]
            logger.debug(f"Another List {g_syms}, {idx}")
        if n == 1 and get_datatype_of_attrib(dep[0][0], dep[0][1]) not in NUMBER_TYPES:
            g_param_list.append([dep[0][1]])
            projected_attrib[idx] = dep[0][1]
            return [[1]]

        coeff = np.zeros((2 ** n, 2 ** n))
        for i in range(n):
            coeff[0][i] = value_used[value_used.index(dep[i][1]) + 1]
        temp_array = get_param_values_external(coeff[0][:n])
        for i in range(2 ** n - 1):
            # Given the values of the n dependencies, we form the rest 2^n - n combinations
            coeff[0][i] = temp_array[i]
        coeff[0][2 ** n - 1] = 1

        local_param_list = get_param_list([i[1] for i in dep])
        g_param_list.append(local_param_list)

        __infinite_loop(coeff, dep)
        print("N", n)
        b = np.zeros((2 ** n, 1))
        for i in range(2 ** n):
            begin_transaction()
            for j in range(n):
                col = g_param_list[idx][j]
                value = coeff[i][j]
                joined_cols = get_other_attribs_in_eqjoin_grp(None, col)
                joined_cols.append((find_table(col), col))
                for j_c in joined_cols:
                    update_attrib_in_table(j_c[1], value, j_c[0])

            exe_result, _ = ctx.connection.sql(ctx.hidden_query)
            rollback()
            if not len(exe_result) == 0:
                b[i][0] = exe_result[0][idx] # self.app.get_attrib_val(exe_result, idx)
        solution = np.linalg.solve(coeff, b)
        solution = np.around(solution, decimals=2)
        final_res = 0
        for i, ele in enumerate(g_syms[idx]):
            final_res += (ele * solution[i])
        final_res += 1 * solution[-1]
        logger.debug(f"Equation A = {coeff}, b = {b}")
        logger.debug(f"Solution {solution}")
        res_expr = nsimplify(collect(final_res, local_symbol_list))
        projected_attrib[idx] = str(round_expr(res_expr, 2))
        return solution


    def find_solution_on_multi(projected_attrib, projection_dep, query):
        solution = []

        for idx_pro, ele in enumerate(projected_attrib):
            if projection_dep[idx_pro] == [] or (
                    len(projection_dep[idx_pro]) < 2 and projection_dep[idx_pro][0][0] == IDENTICAL_EXPR):
                logger.debug("Simple Projection, Continue")
                # Identical output column, so append empty list and continue
                solution.append([])
                g_param_list.append([])
                g_syms.append([])
            else:
                value_used = construct_value_used_with_dmin()
                solution.append(get_solution(projected_attrib, projection_dep, idx_pro, value_used, query))
        return solution

    def build_equation(projected_attrib, projection_dep, projection_sol):
        # print("Full list", self.param_list)
        for idx_pro, ele in enumerate(projected_attrib):
            if projection_dep[idx_pro] == [] or projection_sol[idx_pro] == []:
                continue
            else:
                projected_attrib[idx_pro] = build_equation_helper(projection_sol[idx_pro], g_param_list[idx_pro])

    def build_equation_helper(solution, param_l):
        res_str = ""
        for i in range(len(solution)):
            if solution[i][0] == 0:
                continue
            if res_str == "":
                res_str += (str(solution[i][0]) + "*" + param_l[i]) if solution[i][0] != 1 else param_l[i]
            elif i == len(solution) - 1:
                if solution[i][0] > 0:
                    res_str += "+"
                res_str += str(solution[i][0])
            else:
                if solution[i][0] > 0:
                    res_str += "+"
                res_str += (str(solution[i][0]) + "*" + param_l[i]) if solution[i][0] != 1 else param_l[i]

        return res_str


    logger.info('Starting Projection extractor')
    s_values = []
    projected_attrib, projection_names, projection_dep, check = find_projection_deps(ctx.hidden_query, s_values)
    if if_dependencies_found_incomplete(projection_names, projection_dep):
        for s_v in s_values:
            if s_v[2] is not None:
                update_with_val(s_v[1], s_v[0], s_v[2])
    projected_attrib, projection_names, projection_dep, check = find_projection_deps(ctx.hidden_query, s_values)
    if not check:
        logger.error("Some problem while identifying the dependency list!")
        raise RuntimeError("Some problem while identifying the dependency list!")

    projection_sol = find_solution_on_multi(projected_attrib, projection_dep, ctx.hidden_query)

    ctx.set_projection_extractor(projected_attrib, projection_names, projection_dep, projection_sol)

    logger.debug(projected_attrib)
    logger.info('Finished Projection extractor')
