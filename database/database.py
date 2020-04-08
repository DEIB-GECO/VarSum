from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.engine import Engine, Connection, ResultProxy
from sqlalchemy import exc as sqlalchemy_exceptions
from sqlalchemy import select
from typing import Callable, Optional
from database import db_utils
from loguru import logger

db_engine: Engine


def config_db_engine_parameters(flask_app, db_user, db_password, db_port):
    # configure default database (add more if needed: https://flask-sqlalchemy.palletsprojects.com/en/2.x/binds/)
    flask_app.config['SQLALCHEMY_DATABASE_URI'] = f'postgresql://{db_user}:{db_password}@localhost:{db_port}/gmql_meta_new16_tommaso'
    flask_app.config['SQLALCHEMY_ECHO'] = False
    flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # event-driven system of SQLAlchemy increases overhead
    sql_alchemy_app = SQLAlchemy(flask_app)
    global db_engine
    db_engine = sql_alchemy_app.engine
    logger.debug('db_engine_configured')


def config_db_engine_for_tests(db_user, db_password, db_port):
    import sqlalchemy
    global db_engine
    db_engine = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@localhost:{db_port}/gmql_meta_new16_tommaso')
    logger.debug('db_engine_configured')


def check_and_get_connection(num_attempts: int = 2) -> Connection:
    # following instruction can raise OperationalError if the database is not reachable/not connected but it's caught elsewhere
    connection = db_engine.connect().execution_options(autocommit=True)
    try:
        num_attempts -= 1
        connection.scalar(select([1]))
        logger.debug(f'Connection with pre-ping {str(db_engine.pool.status())}')
        return connection
    except sqlalchemy_exceptions.DatabaseError as e:  # pooled database connection has been invalidated/restarted
        logger.debug('Connection has been reset. Invalidate connection pool.')
        db_engine.dispose()
        logger.debug(f'POOL STATUS {str(db_engine.pool.status())}')
        if num_attempts > 0:
            logger.debug('Attempt {} more time(s)'.format(num_attempts))
            return check_and_get_connection(num_attempts)
        else:
            raise e


def try_stmt(what, log_function: Optional[Callable], log_title: Optional[str], num_attempts: int = 2) -> ResultProxy:
    # following instruction can raise OperationalError if the database is not reachable/not connected but it's caught elsewhere
    connection = db_engine.connect().execution_options(autocommit=True)
    try:
        num_attempts -= 1
        if log_function is not None:
            db_utils.show_stmt(connection, what, log_function, log_title)
        result = connection.execute(what)
        return result
    except sqlalchemy_exceptions.DatabaseError as e:  # pooled database connection has been invalidated/restarted
        logger.debug('Connection has been reset. Invalidate connection pool.')
        db_engine.dispose()
        logger.debug(f'POOL STATUS {str(db_engine.pool.status())}')
        if num_attempts > 0:
            logger.debug('Attempt {} more time(s)'.format(num_attempts))
            return try_stmt(what, log_function, log_title, num_attempts)
        else:
            raise e
    finally:
        connection.close()


def try_py_function(func, num_attempts: int = 2):
    # following instruction can raise OperationalError if the database is not reachable/not connected but it's caught elsewhere
    connection = db_engine.connect().execution_options(autocommit=True)
    try:
        num_attempts -= 1
        result = func(connection)
        return result
    except sqlalchemy_exceptions.DatabaseError as e:  # pooled database connection has been invalidated/restarted
        logger.debug('Connection has been reset. Invalidate connection pool.')
        db_engine.dispose()
        logger.debug(f'POOL STATUS {str(db_engine.pool.status())}')
        if num_attempts > 0:
            logger.debug('Attempt {} more time(s)'.format(num_attempts))
            return try_py_function(func, num_attempts)
        else:
            raise e
    finally:
        connection.close()
