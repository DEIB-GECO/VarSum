import connexion
import mutation_adt
from flask import redirect
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import exc as sqlalchemy_exceptions
from database.functions import DBFunctions
from database.db_entities import *
from typing import Optional
import traceback
import sys


db_user = sys.argv[1]
db_password = sys.argv[2]

connections_counter: int = 0    # never decremented
connections_invalidated: list = list()  # numbers of the connections invalidated


class ReqParamKeys:
    META = 'meta'
    GENDER = 'gender'
    HEALTH_STATUS = 'health_status'
    DNA_SOURCE = 'dna_source'
    ASSEMBLY = 'assembly'
    POPULATION_CODE = 'population'
    SUPER_POPULATION_CODE = 'super_population'
    VARIANTS = 'variants'
    WITH_VARIANTS = 'with'
    WITH_VARS_ON_SAME_CHROM_COPY = 'on_same_chrom_copy'
    WITH_VARS_ON_DIFF_CHROM_COPY = 'on_diff_chrom_copy'


connexion_app = connexion.App(__name__, specification_dir='./')  # internally it starts flask
flask_app = connexion_app.app
# configure parameters for the creation of a SQLAlchemy app instance (later)
flask_app.config['SQLALCHEMY_ECHO'] = False  # echoes SQL statements executed - I prefer my own echoing mechanism
DB_LOG_STATEMENTS = True
flask_app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://{0}:{1}@localhost:15432/gmql_meta_new16_tommaso'.format(db_user, db_password)
flask_app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # turn off event-driven system of SQLAlchemy reducing overhead
flask_app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {'pool_recycle': 300}  # reset 5 minutes old connections
# creates SQLAlchemy instance
sqlalchemy_app = SQLAlchemy(flask_app)
# configure connection pooling and shared engine objects
db_engine = sqlalchemy_app.engine

# ###########################       TRANSFORM INPUT


def prepare_parameters(body):
    meta_attrs = body.get(ReqParamKeys.META)
    meta, regions = None, None   # DBFunctions accepts None arguments
    if meta_attrs is not None:
        meta = MetadataAttrs(meta_attrs.get(ReqParamKeys.GENDER),
                             meta_attrs.get(ReqParamKeys.HEALTH_STATUS),
                             meta_attrs.get(ReqParamKeys.DNA_SOURCE),
                             meta_attrs.get(ReqParamKeys.ASSEMBLY),
                             meta_attrs.get(ReqParamKeys.POPULATION_CODE),
                             meta_attrs.get(ReqParamKeys.SUPER_POPULATION_CODE))
    region_attrs = body.get(ReqParamKeys.VARIANTS)
    if region_attrs is not None:
        regions = RegionAttrs(parse_to_mutation_array(region_attrs.get(ReqParamKeys.WITH_VARIANTS)),
                              parse_to_mutation_array(region_attrs.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY)),
                              parse_to_mutation_array(region_attrs.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY)))
    return meta, regions


def parse_to_mutation_array(dict_array_of_mutations):
    """
    We receive from the user only standard python data structures (generated from the JSON body request parameter).
    We want to convert each dictionary representing a mutation into a Mutation object.
    :param dict_array_of_mutations: the array of dictionary elements, each one representing a mutation.
    :return: an array of Mutation objects.
    """
    if dict_array_of_mutations is None or len(dict_array_of_mutations) == 0:
        return None
    else:
        return [mutation_adt.from_dict(a_dict) for a_dict in dict_array_of_mutations]


# ###########################       ENDPOINTS


@connexion_app.route('/')
def home():
    return redirect('ui/')


def values(items):
    # 1st implementation runs a select distinct in the database for every attribute
    def go(db_functions: DBFunctions):
        # input is already whitelisted
        results = [db_functions.distinct_values_for(item) for item in items]
        # put in serializable format
        serializable_results = {}
        for result_proxy in results:
            attribute_name = result_proxy.keys()[0]
            attribute_values = [row.values()[0] for row in result_proxy.fetchall()]
            serializable_results[attribute_name] = attribute_values
        return serializable_results

    # return try_and_catch(go)

    # since an attribute can also be mut_type which is not indexed, answering takes forever. This is an easy solution
    # considered that the underlying data is updated rarely, we don't need an index on mut_type.

    distinct_values = {
        'assembly': [
            'hg19',
            'GRCh38'
        ],
        'dna_source': [
            'lcl',
            # '',
            'blood'
        ],
        'gender': [
            'female',
            'male'
        ],
        'population': [
            'ITU',
            'ASW',
            'ACB',
            'MXL',
            'CHB',
            'GWD',
            'CLM',
            'YRI',
            'PUR',
            'GIH',
            'TSI',
            'BEB',
            'IBS',
            'MSL',
            'PEL',
            'LWK',
            'ESN',
            'PJL',
            'GBR',
            'JPT',
            'STU',
            'CHS',
            'KHV',
            'CEU',
            'FIN',
            'CDX'
        ],
        'super_population': [
            'EAS',
            'AFR',
            'EUR',
            'AMR',
            'SAS'
        ],
        'mut_type': ['SNP', 'DEL', 'INS', 'CNV', 'MNP', 'SVA', 'ALU', 'LINE1']
    }
    result = {}
    for item in items:
        result[item] = distinct_values[item]
    return result


def samples_distribution(body):
    def go(db_functions: DBFunctions):
        params = prepare_parameters(body)
        result = db_functions.count_samples_by_dimensions(params[0], params[1])
        print('response contains {} rows'.format(result.rowcount))
        marshalled = result_proxy_as_dict(result)
        return marshalled

    return try_and_catch(go)


def variant_distribution(body, **query_params):
    def go(db_functions: DBFunctions):
        by = query_params.pop('by')
        variant = mutation_adt.from_dict(query_params)
        params = prepare_parameters(body)
        result = db_functions.distribution_of_variant(params[0], params[1], variant, by)
        marshalled = result_proxy_as_dict(result)
        return marshalled

    return try_and_catch(go)


def most_common_mutations(body):
    def go(db_functions: DBFunctions):
        params = prepare_parameters(body)
        result = db_functions.most_common_mut_in_sample_set(params[0], params[1])
        marshalled = result_proxy_as_dict(result)
        return marshalled

    return try_and_catch(go)


def rarest_mutations(body):
    def go(db_functions: DBFunctions):
        params = prepare_parameters(body)
        result = db_functions.rarest_mut_in_sample_set(params[0], params[1])
        marshalled = result_proxy_as_dict(result)
        return marshalled

    return try_and_catch(go)


# def are_mutations_unique_between_filter_groups(regions: dict) -> bool:
#     all_mutations = list()
#     if regions.get(ReqParamKeys.WITH_VARIANTS) is not None:
#         all_mutations.append(regions.get(ReqParamKeys.WITH_VARIANTS))
#     if regions.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY) is not None:
#         all_mutations.append(regions.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY))
#     if regions.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY) is not None:
#         all_mutations.append(regions.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY))
#     for region in all_mutations:
#         count = 0
#         for region2 in all_mutations:
#             if region == region2:
#                 count += 1
#         if count > 1:
#             return False
#     return True


# ###########################       TRANSFORM OUTPUT


def result_proxy_as_dict(result_proxy):
    return {
            'columns': result_proxy.keys(),
            'rows': [row.values() for row in result_proxy.fetchall()]
        }

# ###########################       ERROR HANDLING


def try_and_catch(function):
    try:
        db = get_and_config_db_functions(connections_counter-len(connections_invalidated)+2)
        result = function(db)
        db.disconnect()
        return result
    except sqlalchemy_exceptions.OperationalError:  # database connection not available / user canceled query
        log_exception('Connection to the database not available or the query has been canceled by the user.')
        return service_unavailable_message()
    except sqlalchemy_exceptions.NoSuchTableError:
        log_exception('Table not found. Check implementation')
        return service_unavailable_message()
    except sqlalchemy_exceptions.SQLAlchemyError:
        log_exception('Exception from SQLAlchemy. Check implementation')
        return service_unavailable_message()
    except KeyError:
        log_exception(additional_details=None)
        return service_unavailable_message()
    except Exception:
        log_exception("Generic Exception occurred. Look if you can recover from this.")
        return service_unavailable_message()


def get_and_config_db_functions(num_attempts: int = 2) -> DBFunctions:
    global connections_counter
    global connections_invalidated
    print('connections: created {} - invalidated '.format(connections_counter), connections_invalidated)
    # get connection from pool
    connection = db_engine.connect().execution_options(autocommit=True)
    if connection.info.get('connections_counter') is None:
        connections_counter += 1
        connection.info['connections_counter'] = connections_counter
    print('got connection with number {} from connection pool'.format(connection.info.get('connections_counter')))
    try:
        num_attempts -= 1
        db = DBFunctions(connection)
        if DB_LOG_STATEMENTS:
            db.log_sql_commands = True
        return db
    except sqlalchemy_exceptions.DatabaseError:  # pooled database connection has been invalidated/restarted
        print('DB connection # {} has been invalidated/restarted. Pooled connection is being invalidated'
              .format(connection.info['connections_counter']))
        this_connection_num = connection.info['connections_counter']
        connection.invalidate()
        connections_invalidated.append(this_connection_num)
        # 2nd attempt
        if num_attempts > 0:
            print('Refreshing connection...')
            return get_and_config_db_functions(num_attempts)


def log_exception(additional_details: Optional[str]):
    if additional_details is not None:
        print(additional_details)
    traceback.print_exc()


def service_unavailable_message():
    return '503: Service unavailable. Retry later.', 503, {'x-error': 'service unavailable'}


def bad_request_message():
    return '400: Cannot answer to this request.', 400, {'x-error': 'Cannot answer to this request'}


# do this only after the declaration of the api endpoint handlers
connexion_app.add_api('api_definition.yml')  # <- yml located inside the specification dir

if __name__ == '__main__':
    connexion_app.run(host='127.0.0.1', port=5000, debug=True)
