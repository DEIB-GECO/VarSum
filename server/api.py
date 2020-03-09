import connexion
import mutation_adt
from flask import redirect
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import exc as sqlalchemy_exceptions
from database.functions import DBFunctions
from datetime import datetime
from typing import Optional
import traceback
import sys


db_user = sys.argv[1]
db_password = sys.argv[2]
output_redirect = sys.stdout

connections_counter = 0

class ReqParamKeys:
    META = 'meta'
    GENDER = 'gender'
    HEALTH_STATUS = 'health_status'
    DNA_SOURCE = 'dna_source'
    ASSEMBLY = 'assembly'
    POPULATION_CODE = 'population_code'
    SUPER_POPULATION_CODE = 'super_population_code'
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
# creates SQLAlchemy instance
sqlalchemy_app = SQLAlchemy(flask_app)
# configure connection pooling and shared engine objects
db_engine = sqlalchemy_app.engine


@connexion_app.route('/')
def home():
    return redirect('ui/')


# ENDPOINT for /individuals #TODO change this name
def individuals(body):
    def go(with_param):
        meta_attrs = with_param.get(ReqParamKeys.META)
        region_attrs = with_param.get(ReqParamKeys.VARIANTS)
        db_functions = get_and_config_db_functions()
        # apply filter criteria
        samples_with_meta_attrs_t_name = create_meta_view(db_functions, meta_attrs) or db_functions.default_metadata_table_name
        samples_with_region_attrs_t_name = create_region_table(db_functions, region_attrs)
        # compute result
        if samples_with_region_attrs_t_name is None:
            result = db_functions.count_samples_by_dimensions(
                samples_with_meta_attrs_t_name or db_functions.default_metadata_table_name,
                None,
                'dw')
        else:
            result = db_functions.mutation_frequency_by_dimensions(
                samples_with_meta_attrs_t_name,
                samples_with_region_attrs_t_name,
                'dw')
        print('response contains {} rows'.format(result.rowcount), file=output_redirect)
        marshalled = result_proxy_as_dict(result)
        if samples_with_meta_attrs_t_name is not None:
            db_functions.drop_view(samples_with_meta_attrs_t_name, 'dw')
        db_functions.disconnect()
        return marshalled

    return try_and_catch(go, body)


# ENDPOINT for most-common mutations #TODO change this name
def most_common_mutations(body):
    def go(with_param):
        meta_attrs = with_param.get(ReqParamKeys.META)
        region_attrs = with_param.get(ReqParamKeys.VARIANTS)
        db_functions = get_and_config_db_functions()
        # apply filter criteria
        samples_with_meta_attrs_t_name = create_meta_view(db_functions,
                                                          meta_attrs) or db_functions.default_metadata_table_name
        samples_with_region_attrs_t_name = create_region_table(db_functions, region_attrs)
        # compute result
        result = db_functions.most_common_mut_in_sample_set(samples_with_meta_attrs_t_name,
                                                            samples_with_region_attrs_t_name,
                                                            'dw')
        marshalled = result_proxy_as_dict(result)
        db_functions.disconnect()
        return marshalled

    return try_and_catch(go, body)


# ENDPOINT for rarest mutations #TODO change this name
def rarest_mutations(body):
    def go(with_param):
        meta_attrs = with_param.get(ReqParamKeys.META)
        region_attrs = with_param.get(ReqParamKeys.VARIANTS)
        db_functions = get_and_config_db_functions()
        # apply filter criteria
        samples_with_meta_attrs_t_name = create_meta_view(db_functions,
                                                          meta_attrs) or db_functions.default_metadata_table_name
        samples_with_region_attrs_t_name = create_region_table(db_functions, region_attrs)
        # compute result
        result = db_functions.rarest_mut_in_sample_set(samples_with_meta_attrs_t_name,
                                                       samples_with_region_attrs_t_name,
                                                       'dw')
        marshalled = result_proxy_as_dict(result)
        db_functions.disconnect()
        return marshalled

    return try_and_catch(go, body)


def result_proxy_as_dict(result_proxy):
    return {
            'columns': result_proxy.keys(),
            'rows': [row.values() for row in result_proxy.fetchall()]
        }


def get_and_config_db_functions() -> DBFunctions:
    # get connection from pool
    connection = db_engine.connect().execution_options(autocommit=True)
    global connections_counter
    if connection.info.get('connections_counter') is None:
        connections_counter += 1
        connection.info['connections_counter'] = connections_counter
    print('got connection with number {} from connection pool'.format(connection.info.get('connections_counter')))
    db = DBFunctions(connection)
    if DB_LOG_STATEMENTS:
        db.log_sql_commands = True
    return db


def create_meta_view(db: DBFunctions, meta_attrs: dict) -> Optional[str]:
    """
    :param db: a DBFunctions
    :param meta_attrs: the dictionary of metadata attributes the user wants in the sample set
    :return: Either the name of a view from schema 'dw' describing all the individuals with the required metadata
    attributes or None when meta_attrs is None.
    """
    if meta_attrs is None:
        return None
    else:
        new_meta_table_name = 'meta_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
        db.view_of_samples_with_metadata(new_meta_table_name,
                                         meta_attrs.get(ReqParamKeys.GENDER),
                                         meta_attrs.get(ReqParamKeys.HEALTH_STATUS),
                                         meta_attrs.get(ReqParamKeys.DNA_SOURCE),
                                         meta_attrs.get(ReqParamKeys.ASSEMBLY),
                                         meta_attrs.get(ReqParamKeys.POPULATION_CODE),
                                         meta_attrs.get(ReqParamKeys.SUPER_POPULATION_CODE))
        return new_meta_table_name


def create_region_table(db: DBFunctions, region_attrs: dict) -> Optional[str]:
    """
    :param db: an insatnce of DBFunctions
    :param region_attrs: the dictionary of the properties that the variants in the sample set must have
    :return: Either the name of the table containing the individuals (+ the given regions) or None when region_attrs is
    None or empty.
    """
    if region_attrs is None:
        return None
    else:
        # compute each filter on regions separately
        partial_result_table_names = list()
        if region_attrs.get(ReqParamKeys.WITH_VARIANTS):
            table_name = db.random_t_name_w_prefix('region_with')
            db.table_with_all_of_mutations(table_name,
                                           'dw',
                                           *parse_to_mutation_array(region_attrs[ReqParamKeys.WITH_VARIANTS]))
            partial_result_table_names.append(table_name)
        if region_attrs.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY):
            table_name = db.random_t_name_w_prefix('region_same_chrom_copy')
            db.table_mutations_on_same_chrom_copy(table_name,
                                                  'dw',
                                                  *parse_to_mutation_array(region_attrs[ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY]))
            partial_result_table_names.append(table_name)
        if region_attrs.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY):
            table_name = db.random_t_name_w_prefix('region_diff_chrom_copy')
            db.table_mutations_on_different_chrom_copies(table_name,
                                                         'dw',
                                                         *parse_to_mutation_array(region_attrs[ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY]))
            partial_result_table_names.append(table_name)
        if len(partial_result_table_names) == 0:    # when no filter on regions is applied
            return None
        elif len(partial_result_table_names) == 1:  # when only one filter kind
            return partial_result_table_names[0]
        else:                                       # put together all filters in AND
            intersection_t_name = db.random_t_name_w_prefix('intersect')
            db.take_regions_of_common_individuals(intersection_t_name,
                                                  'dw',
                                                  partial_result_table_names,
                                                  ['dw'] * len(partial_result_table_names))
            return intersection_t_name


def are_mutations_unique_between_filter_groups(regions: dict) -> bool:
    all_mutations = list()
    if regions.get(ReqParamKeys.WITH_VARIANTS) is not None:
        all_mutations.append(regions.get(ReqParamKeys.WITH_VARIANTS))
    if regions.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY) is not None:
        all_mutations.append(regions.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY))
    if regions.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY) is not None:
        all_mutations.append(regions.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY))
    for region in all_mutations:
        count = 0
        for region2 in all_mutations:
            if region == region2:
                count += 1
        if count > 1:
            return False
    return True


def parse_to_mutation_array(dict_array_of_mutations):
    """
    We receive from the user only standard python data structures (generated from the JSON body request parameter).
    We want to convert each dictionary representing a mutation into a Mutation object.
    :param dict_array_of_mutations: the array of dictionary elements, each one representing a mutation.
    :return: an array of Mutation objects.
    """
    return [mutation_adt.from_dict(a_dict) for a_dict in dict_array_of_mutations]


def service_unavailable_message():
    return '503: Service unavailable. Retry later.', 400, {'x-error': 'service unavailable'}


def bad_request_message():
    return '400: Cannot answer to this request.', 400, {'x-error': 'Cannot answer to this request'}


def try_and_catch(function, parameter):
    try:
        return function(parameter)
    except sqlalchemy_exceptions.DBAPIError:
        log_exception('Exception from DBAPI. Connection to the database may be not available.')
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
        log_exception(additional_details=None)
        return service_unavailable_message()


def log_exception(additional_details: Optional[str]):
    if additional_details is not None:
        print(additional_details, file=output_redirect)
    traceback.print_exc(file=output_redirect)


# do this only after the declaration of the api endpoint handlers
connexion_app.add_api('api_definition.yml')  # <- yml located inside the specification dir

if __name__ == '__main__':
    connexion_app.run(host='127.0.0.1', port=5000, debug=True)
