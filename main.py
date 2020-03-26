import sys
import database.database as database
from loguru import logger

run = sys.argv[1]
db_user = sys.argv[2]
db_password = sys.argv[3]

logger.add("./logs/log_{time}.log",
           rotation='100 MB',
           format='{time:YYYY-MM-DD at HH:mm:ss} | {level} | {message}',
           backtrace=True,
           diagnose=True)

if __name__ == '__main__':
    if run == 'server':
        from server import api
        database.config_db_engine_parameters(api.flask_app, db_user, db_password)
        api.run()
    elif run == 'tests':
        database.config_db_engine_for_tests(db_user, db_password)
        import tests.tests                              # this runs anything is in the tests.py module
    else:
        logger.critical('The first program argument must be either "server" or "tests" followed by database username and password.')
