import connexion
from data_sources.io_parameters import *
from flask import redirect
from data_sources.coordinator import Coordinator, AskUserIntervention, NoDataFromSources
import sqlalchemy.exc
from prettytable import PrettyTable
from loguru import logger


class ReqParamKeys:
    """
    This class maps the attribute names used in the api_definition.yml to constants used in this module only.
    """
    META = 'having_meta'
    OF = 'of'  # is used as alias of having_meta in the context of: find variants in region
    GENDER = 'gender'
    HEALTH_STATUS = 'health_status'
    DNA_SOURCE = 'dna_source'
    ASSEMBLY = 'assembly'
    POPULATION_CODE = 'population'
    SUPER_POPULATION_CODE = 'super_population'
    ETHNICITY = 'ethnicity'
    DISEASE = 'disease'

    VARIANTS = 'having_variants'
    WITH_VARIANTS = 'with'
    WITH_VARS_ON_SAME_CHROM_COPY = 'on_same_chrom_copy'
    WITH_VARS_ON_DIFF_CHROM_COPY = 'on_diff_chrom_copy'
    WITH_VARS_IN = 'in'
    WITH_VARS_IN_CELL_TYPE = 'in_cell_type'
    WITHOUT_VARIANT = 'without'

    OUTPUT = 'filter_output'
    OUT_MIN_FREQUENCY = 'min_frequency'
    OUT_MAX_FREQUENCY = 'max_frequency'
    OUT_LIMIT = 'limit'

    BY_ATTRIBUTES = 'group_by'

    TARGET_VARIANT = 'target_variant'

    INCLUDE_DOWNLOAD_URL = 'donors_download_url'

    CHROM = 'chrom'
    START = 'start'
    STOP = 'stop'
    STRAND = 'strand'
    VAR_ID = 'id'
    REF = 'ref'
    ALT = 'alt'

    GENE_NAME = 'name'
    GENE_TYPE_IN_GENE_OBJECT = 'type'
    GENE_TYPE_IN_VALUES_ENDPOINT = 'gene_type'
    GENE_ID = 'ensemble_id'

    GEN_VAR_SOURCES = 'source'


connexion_app = connexion.App(__name__, specification_dir='./')  # internally it starts flask
flask_app = connexion_app.app
base_path = '/popstudy/'
api_doc_relative_path = 'api/ui/'
request_incremental_index = 0   # used to identify every new request


def run():
    # do this only after the declaration of the api endpoint handlers
    connexion_app.add_api('api_definition.yml')  # <- yml located inside the specification dir
    connexion_app.run(host='localhost',
                      port=51992,
                      debug=True,
                      threaded=True,        # this is True by default of Flask - I just wanna to make it explicit
                      use_reloader=False)   # prevents module main from starting twice, but disables auto-reload upon changes detected


# ###########################       ENDPOINTS
def donor_grouping(body):
    def go():
        req_logger.info(f'new request to /donor_distribution with request_body: {body}')
        params = prepare_body_parameters(body)
        result = Coordinator(req_logger, params[8]).donor_distribution(params[2], params[0], params[1], params[7])
        return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


def variant_grouping(body):
    def go():
        req_logger.info(f'new request to /variant_distribution with request_body: {body}')
        params = prepare_body_parameters(body)
        result = Coordinator(req_logger, params[8]).variant_distribution(params[2], params[0], params[1], params[3])
        return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


def most_common_variants(body):
    def go():
        req_logger.info(f'new request to /most_common_variants with request_body: {body}')
        params = prepare_body_parameters(body)
        result = Coordinator(req_logger, params[8]).rank_variants_by_freq(params[0], params[1], False, params[6], params[5])
        return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


def rarest_variants(body):
    def go():
        req_logger.info(f'new request to /rarest_variants with request_body: {body}')
        params = prepare_body_parameters(body)
        result = Coordinator(req_logger, params[8]).rank_variants_by_freq(params[0], params[1], True, params[4], params[5])
        return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


def values(attribute):
    def go():
        req_logger.info(f'new request to /values with attribute {attribute}')
        item = parse_name_to_vocabulary(attribute)
        if item is None:
            req_logger.info('response says the attribute is not valid')
            return f'Attribute {attribute} is not a valid parameter for this request', 400
        else:
            result = Coordinator(req_logger).values_of_attribute(item)
            return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


def annotate(body):
    def go():
        req_logger.info(f'new request to /annotate with request_body: {body}')
        if body.get(ReqParamKeys.STOP):
            interval = parse_genomic_interval_from_dict(body)
            result = Coordinator(req_logger).annotate_interval(interval, body.get(ReqParamKeys.ASSEMBLY))
        else:
            variant = parse_variant_from_dict(body)
            result = Coordinator(req_logger).annotate_variant(variant, body.get(ReqParamKeys.ASSEMBLY))
        return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


def variants_in_region(body):
    def go():
        req_logger.info(f'new request to /variants_in_region with request_body: {body}')
        optional_params = prepare_body_parameters(body)
        if body.get(ReqParamKeys.STOP):
            interval = parse_genomic_interval_from_dict(body)
            result = Coordinator(req_logger, optional_params[8]) \
                .variants_in_genomic_interval(interval, optional_params[0], optional_params[1])
        else:
            gene = parse_gene_from_dict(body)
            result = Coordinator(req_logger, optional_params[8])\
                .variants_in_gene(gene, optional_params[0], optional_params[1])
        return result
    req_logger = unique_logger()
    return try_and_catch(go, req_logger)


@connexion_app.route(base_path)
def home():
    # redirect to base_path + api_doc_relative_path
    unique_logger().info('new request to /home')
    return redirect(api_doc_relative_path)


# ###########################       TRANSFORM INPUT
def prepare_body_parameters(body):
    var_sources = body.get(ReqParamKeys.GEN_VAR_SOURCES)
    meta = body.get(ReqParamKeys.META) or body.get(ReqParamKeys.OF)
    if meta is not None:
        meta = MetadataAttrs(gender=meta.get(ReqParamKeys.GENDER),
                             health_status=meta.get(ReqParamKeys.HEALTH_STATUS),
                             dna_source=meta.get(ReqParamKeys.DNA_SOURCE),
                             assembly=meta.get(ReqParamKeys.ASSEMBLY),
                             population=meta.get(ReqParamKeys.POPULATION_CODE),
                             super_population=meta.get(ReqParamKeys.SUPER_POPULATION_CODE),
                             ethnicity=meta.get(ReqParamKeys.ETHNICITY),
                             disease=meta.get(ReqParamKeys.DISEASE))

    variants = body.get(ReqParamKeys.VARIANTS) or \
               (body.get(ReqParamKeys.OF).get(ReqParamKeys.VARIANTS) if body.get(ReqParamKeys.OF) else None)
    if variants is not None:
        if variants.get(ReqParamKeys.WITH_VARS_IN) is not None:
            if variants[ReqParamKeys.WITH_VARS_IN].get(ReqParamKeys.STOP):
                interval = parse_genomic_interval_from_dict(variants[ReqParamKeys.WITH_VARS_IN])
                gene = None
            else:
                gene = parse_gene_from_dict(variants[ReqParamKeys.WITH_VARS_IN])
                interval = None
        else:
            interval = None
            gene = None
        variants = RegionAttrs(parse_to_mutation_array(variants.get(ReqParamKeys.WITH_VARIANTS)),
                               parse_to_mutation_array(variants.get(ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY)),
                               parse_to_mutation_array(variants.get(ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY)),
                               interval,
                               gene,
                               variants.get(ReqParamKeys.WITH_VARS_IN_CELL_TYPE),
                               parse_to_mutation_array(variants.get(ReqParamKeys.WITHOUT_VARIANT)))

    by_attributes_usr_input = body.get(ReqParamKeys.BY_ATTRIBUTES)
    by_attributes = [parse_name_to_vocabulary(att) for att in by_attributes_usr_input] if by_attributes_usr_input else None

    target_variant = body.get(ReqParamKeys.TARGET_VARIANT)
    if target_variant is not None:
        target_variant = parse_variant_from_dict(target_variant)

    out_limit = None
    out_min_frequency = None
    out_max_frequency = None
    output = body.get(ReqParamKeys.OUTPUT)
    if output is not None:
        out_limit = output.get(ReqParamKeys.OUT_LIMIT)
        out_max_frequency = output.get(ReqParamKeys.OUT_MAX_FREQUENCY)
        out_min_frequency = output.get(ReqParamKeys.OUT_MIN_FREQUENCY)

    include_download_url = body.get(ReqParamKeys.INCLUDE_DOWNLOAD_URL) or False

    return meta, variants, by_attributes, target_variant, out_min_frequency, out_limit, out_max_frequency, include_download_url, var_sources


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
        return [parse_variant_from_dict(a_dict) for a_dict in dict_array_of_mutations]


def parse_variant_from_dict(mutation_dict: dict):
    if mutation_dict.get(ReqParamKeys.VAR_ID) is not None:
        return Mutation(_id=mutation_dict[ReqParamKeys.VAR_ID])
    else:
        return Mutation(mutation_dict.get(ReqParamKeys.CHROM),
                        mutation_dict.get(ReqParamKeys.START),
                        mutation_dict.get(ReqParamKeys.REF),
                        mutation_dict.get(ReqParamKeys.ALT))


def parse_genomic_interval_from_dict(region_dict: dict):
    return GenomicInterval(region_dict.get(ReqParamKeys.CHROM),
                           region_dict.get(ReqParamKeys.START),
                           region_dict.get(ReqParamKeys.STOP),
                           region_dict.get(ReqParamKeys.STRAND))


def parse_gene_from_dict(gene_dict: dict):
    return Gene(gene_dict.get(ReqParamKeys.GENE_NAME),
                gene_dict.get(ReqParamKeys.GENE_TYPE_IN_GENE_OBJECT),
                gene_dict.get(ReqParamKeys.GENE_ID))


def parse_name_to_vocabulary(name: str):
    if name == ReqParamKeys.GENDER:
        return Vocabulary.GENDER
    elif name == ReqParamKeys.POPULATION_CODE:
        return Vocabulary.POPULATION
    elif name == ReqParamKeys.SUPER_POPULATION_CODE:
        return Vocabulary.SUPER_POPULATION
    elif name == ReqParamKeys.DNA_SOURCE:
        return Vocabulary.DNA_SOURCE
    elif name == ReqParamKeys.HEALTH_STATUS:
        return Vocabulary.HEALTH_STATUS
    elif name == ReqParamKeys.ASSEMBLY:
        return Vocabulary.ASSEMBLY
    elif name == ReqParamKeys.GENE_TYPE_IN_VALUES_ENDPOINT:
        return Vocabulary.GENE_TYPE
    elif name == ReqParamKeys.ETHNICITY:
        return Vocabulary.ETHNICITY
    elif name == ReqParamKeys.DISEASE:
        return Vocabulary.DISEASE
    else:
        logger.info('name without a match in Vocabulary')
        return None


# ###########################       TRANSFORM OUTPUT
def print_output_table(output_dictionary):
    pretty_table = PrettyTable(output_dictionary['columns'])
    for row in output_dictionary['rows']:
        pretty_table.add_row(row)
    print(pretty_table)


# ###########################       ERROR HANDLING
def try_and_catch(function, request_logger, *args, **kwargs):
    # noinspection PyBroadException
    try:
        result = function(*args, **kwargs)
        if result is not None:
            request_logger.success('response ok')
            return result, 200
        else:
            request_logger.error('Coordinator returned None')
            return service_unavailable_message(request_logger)
    except VariantUndefined as e:
        return bad_variant_parameters(e.args[0], request_logger)
    except GenomicIntervalUndefined as e:
        return bad_genomic_interval_parameters(e.args[0], request_logger)
    except ContradictingRegionAttributes as e:
        return contradicting_region_attributes(request_logger)
    except AskUserIntervention as e:
        request_logger.info(f'Asking for user intervention with response {e.proposed_status_code}')
        return e.response_body, e.proposed_status_code
    except NoDataFromSources as e:
        request_logger.warning(f'Sources produced no data. Potential notices: {e.response_body}')
        # don't delete the braces, or Flask can't unpack the result correctly
        return (e.response_body, e.proposed_status_code) if e.response_body is not None else service_unavailable_message(request_logger)
    except sqlalchemy.exc.OperationalError:  # database connection not available / user canceled query
        request_logger.exception('database connection not available / user canceled query')
        return service_unavailable_message(request_logger)
    except Exception:
        request_logger.exception('unknown exception in module api')
        return service_unavailable_message(request_logger)


@flask_app.errorhandler(Exception)
def unhandled_exception(e):
    logger.error('! An uncaught exception reached the default exception handler in module api!')
    logger.exception(e)
    return service_unavailable_message(logger)


def service_unavailable_message(log_with):
    log_with.error('responded with service_unavailable_message')
    return 'Service temporarily unavailable. Retry later.', 503, {'x-error': 'service unavailable'}


def bad_variant_parameters(msg: str, log_with):
    log_with.info('responded with bad_variant_parameters')
    return 'One or more variants included in the request miss required attributes or contain misspells. ' \
           f'Detailed message: {msg}', 400, {'x-error': 'Cannot answer to this request'}


def contradicting_region_attributes(log_with):
    log_with.info('responded with contradicting_region_attributes')
    return f'One or more variants included in the request appear both in {ReqParamKeys.WITHOUT_VARIANT} clause ' \
           f'and in one of the clauses {ReqParamKeys.WITH_VARIANTS}, {ReqParamKeys.WITH_VARS_ON_SAME_CHROM_COPY} or ' \
           f'{ReqParamKeys.WITH_VARS_ON_DIFF_CHROM_COPY}. That is a contradiction.', 400, {'x-error': 'Cannot answer to this request'}


def bad_genomic_interval_parameters(msg: str, log_with):
    log_with.info('responded with bad_genomic_interval_parameters')
    return 'One or more genomic intervals included in the request miss required attributes or contain misspells. ' \
           f'Detailed message: {msg}', 400, {'x-error': 'Cannot answer to this request'}


def unique_logger():
    global request_incremental_index
    request_incremental_index += 1
    return logger.bind(request_id=request_incremental_index)


if __name__ == '__main__':
    run()
    import sys
    from database import database
    db_user = sys.argv[1]
    db_password = sys.argv[2]
    db_port = sys.argv[3]
    database.config_db_engine_parameters(flask_app, db_user, db_password, db_port)


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
