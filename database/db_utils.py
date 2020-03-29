from sqlalchemy import Table, text, select
from sqlalchemy.engine import ResultProxy
from prettytable import PrettyTable
from database import create_view_module, create_table_module
from datetime import datetime


# EXECUTORS
def exec_raw_query(query_string, connection, log_sql_statement: bool, log_function) -> ResultProxy:
    """
    :param query_string: raw SQL query without the trailing ';'
    :param connection
    :param log_sql_statement
    :param log_function a print function or other log function
    :return: a sqlalchemy.engine.ResultProxy object
    """
    _query = text(query_string + ';')
    if log_sql_statement:
        log_function('###      EXECUTE RAW QUERY       ###')
        show_stmt(connection, _query, log_function, None)
    return connection.execute(_query)


def drop_view(name: str, from_schema: str, connection, log_sql_stmt: bool, log_function):
    exec_raw_query('DROP VIEW "' + from_schema + '".' + name, connection, log_sql_stmt, log_function)


def create_table_as(name: str, select_stmt, into_schema, connection, log_sql_stmt: bool, log_function):
    compiled_select = select_stmt.compile(compile_kwargs={"literal_binds": True},
                                          dialect=connection.dialect)
    stmt = 'CREATE TABLE "' + into_schema + '".' + name + ' AS ' + str(compiled_select)
    exec_raw_query(stmt, connection, log_sql_stmt, log_function)


# VISUALIZE RESULTS AND QUERIES
def print_query_result(result: ResultProxy):
    pretty_table = PrettyTable(result.keys())
    row = result.fetchone()
    while row:
        pretty_table.add_row(row)
        row = result.fetchone()
    print(pretty_table)


def show_stmt(connection, stmt, log_function, intro):
    # #substitued by instr below
    if intro is not None:
        log_function('###   ' + intro + '   ###')
    compiled_stmt = stmt.compile(compile_kwargs={"literal_binds": True}, dialect=connection.dialect)
    log_function(str(compiled_stmt))


def print_table_named(connection, db_meta, table_name: str, table_schema: str):
    if not table_name.islower():
        print(
            'Postgre saves table names as lower case strings. I\'ll try to access your table as all lowercase.')
    table_to_print = Table(table_name.lower(), db_meta, autoload=True, autoload_with=connection,
                           schema=table_schema)
    query_all = select([table_to_print])
    result = connection.execute(query_all)
    print_query_result(result)


def print_table(connection, table: Table):
    query_all = select([table])
    result = connection.execute(query_all)
    print_query_result(result)


# SQL STATEMENT GENERATORS
def stmt_create_view_as(name: str, select_stmt, into_schema):
    return create_view_module.CreateView('"' + into_schema + '".' + name, select_stmt)


def stmt_create_table_as(name: str, select_stmt, into_schema):
    return create_table_module.CreateTableAs('"' + into_schema + '".' + name, select_stmt)


# OTHER
def random_t_name_w_prefix(prefix: str):
    return prefix + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
