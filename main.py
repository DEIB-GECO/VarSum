import sys
import database.database as database
from loguru import logger

wrong_arguments_message = 'The first program argument must be either "server" or "tests" followed by database username, ' \
                          'password and port. Lastly, the severity level of the log messages to see on the console.'
try:
    run = sys.argv[1]
    db_user = sys.argv[2]
    db_password = sys.argv[3]
    db_port = sys.argv[4]
    output_log_lvl = sys.argv[5]
except Exception:
    logger.error(wrong_arguments_message)
    sys.exit(1)

logger.remove()  # removes default logger to stderr with level DEBUG
# log to stderr only messages with level INFO or above (exceptions are level ERROR, thus included)
logger.add(sys.stderr,
           level=output_log_lvl,
           format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                  "<blue>{extra[request_id]}</blue> | "
                  "<level>{level: <8}</level> | "
                  "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
           colorize=True,
           backtrace=True,
           diagnose=True)
# log to file any message of any security level
logger.add("./logs/log_{time}.log",
           level='TRACE',
           rotation='100 MB',
           format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                  "<blue>{extra[request_id]}</blue> | "
                  "<level>{level: <8}</level> | "
                  "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
           colorize=False,
           backtrace=True,
           diagnose=True)
logger.configure(
    extra={
        'request_id': 'default'
    }
)

if __name__ == '__main__':
    if run == 'server':
        from server import api

        database.config_db_engine_parameters(api.flask_app, db_user, db_password, db_port)
        api.run()
    elif run == 'tests':
        database.config_db_engine_for_tests(db_user, db_password, db_port)

        # noinspection PyUnresolvedReferences
        import tests.tests                              # this runs anything is in the tests.py module
    else:
        logger.critical(wrong_arguments_message)
