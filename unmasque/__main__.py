import sys
import argparse
from loguru import logger

from .src.connection import PostgresConnection
from .src.context import UnmasqueContext
from .src.pipeline import Pipeline
from .src.query_builder import query_from_context

# Remove default logger
logger.remove(0)
logger.add(sys.stdout, format='{extra[pipeline]} Pipeline | {extra[module]} | <level>{level}</level> | {message}')

def banner() -> str:
    from pyfiglet import Figlet
    f = Figlet(font='slant')
    return str(f.renderText('unmasque2'))

# default_hidden_query = "select c_mktsegment, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, " \
#      "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
#      "and l_orderkey = o_orderkey and o_orderdate <= date '1995-10-13' and l_quantity <= 123 " \
#      "group by o_orderdate, o_shippriority, c_mktsegment having sum(l_extendedprice) >= 212 and sum(l_extendedprice) <= 30000000 " \
#      "order by revenue desc, o_orderdate asc, o_shippriority asc limit 4;"

# default_hidden_query = "select c_mktsegment, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, " \
#      "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
#      "and l_orderkey = o_orderkey and o_orderdate <= date '1995-10-13' " \
#      "group by o_orderdate, o_shippriority, c_mktsegment having sum(l_extendedprice) >= 212 and sum(l_extendedprice) <= 30000000 and min(l_quantity) <= 123 " \
#      "order by revenue desc, o_orderdate asc, o_shippriority asc limit 4;"

# default_hidden_query = "select c_mktsegment, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, " \
#      "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
#      "and l_orderkey = o_orderkey and o_orderdate <= date '1995-10-13' " \
#      "group by o_orderdate, o_shippriority, c_mktsegment having avg(l_extendedprice) >= 212 and avg(l_extendedprice) <= 30000000 and min(l_quantity) <= 123 " \
#      "order by revenue desc, o_orderdate asc, o_shippriority asc limit 4;"

# default_hidden_query = "select ps_partkey from partsupp where ps_availqty <= 20000 group by ps_partkey;";

# default_hidden_query = "select ps_partkey from partsupp where ps_availqty <= 20000;";

default_hidden_query = "select c_mktsegment, sum(l_extendedprice*(1 - l_discount) + l_quantity) as revenue, " \
 "o_orderdate, o_shippriority from customer, orders, lineitem where c_custkey = o_custkey " \
 "and l_orderkey = o_orderkey and o_orderdate <= date '1995-10-13' and l_extendedprice >= 212  and l_extendedprice <= 30000000 and l_quantity <= 123 " \
 "group by o_orderdate, o_shippriority, c_mktsegment " \
 "order by revenue desc, o_orderdate asc, o_shippriority asc;"

# BUG: --> For some reason, having a key in the projection crashes projection extraction
# default_hidden_query = "select l_orderkey, sum(l_extendedprice * (1 - l_discount)) as revenue, o_orderdate, o_shippriority " \
#     " From customer, orders, lineitem " \
#     " Where c_custkey = o_custkey and l_orderkey = o_orderkey and " \
#     " o_orderdate < date '1995-03-15' and l_shipdate > date '1995-03-15' " \
#     " Group By l_orderkey, o_orderdate, o_shippriority " \
#     " Order by revenue desc, o_orderdate " \
#     " Limit 10;"

def run_unmasque():
    conn = PostgresConnection(db_name='tpch', schema='public', host='localhost', port=5432, user='tpch', password='tpch')
    ctx = UnmasqueContext(conn, default_hidden_query)

    with Pipeline(ctx) as pipeline:
        pipeline.run()

    ctx.print_timing()
    print("Hidden query")
    print("===============")
    print(default_hidden_query)
    print("\nExtracted query")
    print("===============")
    print(query_from_context(ctx))


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(
                prog='unmasque2',
                description='TODO :D',
            )

    print(banner(), '\n')

    run_unmasque()



