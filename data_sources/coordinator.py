from data_sources.annot_interface import AnnotInterface
from data_sources.source_interface import *
from data_sources.kgenomes.kgenomes import KGenomes
from data_sources.gencode_v19_hg19.gencode_v19_hg19 import GencodeV19HG19
from typing import List, Type
from sqlalchemy.engine import ResultProxy
from sqlalchemy import select, union, func, literal, column, cast, types, desc, asc, literal_column, text
import sqlalchemy.exc
import database.db_utils as db_utils
import database.database as database
import concurrent.futures
from loguru import logger
import itertools


_sources: List[Type[Source]] = [
    KGenomes
]
_annotation_sources: List[Type[AnnotInterface]] = [
    GencodeV19HG19
]

LOG_SQL_STATEMENTS = True


def donor_distribution(by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs):
    transformed_region_attrs = resolve_gene_name_into_interval(region_attrs)
    if transformed_region_attrs[1] == 200:
        region_attrs = transformed_region_attrs[0]
    else:
        return transformed_region_attrs
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.donors)]

    # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
    by_attributes_copy = by_attributes.copy()
    if Vocabulary.DONOR_ID not in by_attributes_copy:
        by_attributes_copy.append(Vocabulary.DONOR_ID)
    by_attributes_copy.sort(key=lambda x: x.name)

    notices = list()

    # collect results from individual sources
    def ask_to_source(source: Type[Source]):
        def do():
            obj: Source = source()
            available_attributes_in_source = obj.get_available_attributes()

            select_from_source_output = []  # what we select from the source output (both available and unavailable attributes)
            selectable_attributes: List[Vocabulary] = []  # what we can ask to the source to give us
            for elem in by_attributes_copy:
                if elem in available_attributes_in_source:
                    selectable_attributes.append(elem)
                    select_from_source_output.append(column(elem.name))
                else:
                    select_from_source_output.append(cast(literal(Vocabulary.unknown.name), types.String).label(elem.name))

            def donors(a_connection):
                source_stmt = obj.donors(a_connection, selectable_attributes, meta_attrs, region_attrs)\
                    .alias(source.__name__)
                return \
                    select(select_from_source_output) \
                    .select_from(source_stmt)

            return database.try_py_function(donors)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        # aggregate the results of all the queries
        by_attributes_as_columns = [column(att.name) for att in by_attributes]
        stmt = \
            select(
                by_attributes_as_columns +
                [func.count(column(Vocabulary.DONOR_ID.name)).label('DONORS')]
            )\
            .select_from(union(*from_sources).alias("all_sources"))\
            .group_by(func.cube(*by_attributes_as_columns))

        def compute_result(connection: Connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, stmt, logger.debug, 'DONOR DISTRIBUTION')
            result = connection.execute(stmt)
            return result

        return result_proxy_as_dict(database.try_py_function(compute_result)), 200


def variant_distribution(by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, variant: Mutation):
    transformed_region_attrs = resolve_gene_name_into_interval(region_attrs)
    if transformed_region_attrs[1] == 200:
        region_attrs = transformed_region_attrs[0]
    else:
        return transformed_region_attrs
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.variant_occurrence)]

    # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
    by_attributes_copy = by_attributes.copy()
    if Vocabulary.DONOR_ID not in by_attributes_copy:
        by_attributes_copy.append(Vocabulary.DONOR_ID)
    by_attributes_copy.sort(key=lambda x: x.name)

    notices = list()

    # collect results from individual sources as DONOR_ID | OCCURRENCE | <by_attributes>
    def ask_to_source(source: Type[Source]):
        def do():
            obj: Source = source()
            available_attributes_in_source = obj.get_available_attributes()

            select_from_source_output = []  # what we select from the source output (both available and unavailable attributes)
            selectable_attributes: List[Vocabulary] = []  # what we can ask to the source to give us
            for elem in by_attributes_copy:
                if elem in available_attributes_in_source:
                    selectable_attributes.append(elem)
                    select_from_source_output.append(column(elem.name))
                else:
                    select_from_source_output.append(cast(literal(Vocabulary.unknown.name), types.String).label(elem.name))
            select_from_source_output.append(column(Vocabulary.OCCURRENCE.name))

            def variant_occurrence(a_connection):
                source_stmt = obj.variant_occurrence(a_connection, selectable_attributes, meta_attrs, region_attrs, variant)\
                    .alias(source.__name__)
                return \
                    select(select_from_source_output)\
                    .select_from(source_stmt)

            return database.try_py_function(variant_occurrence)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources) + 1) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)
        chromosome = executor.submit(get_chromosome_of_variant, variant).result()

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        by_attributes_as_columns = [column(att.name) for att in by_attributes]

        func_count_donors = func.count(column(Vocabulary.DONOR_ID.name)).label('POPULATION_SIZE')
        func_count_positive_donors = func.count(1).filter(column(Vocabulary.OCCURRENCE.name) > 0).label('POSITIVE_DONORS')
        func_count_occurrence = func.sum(column(Vocabulary.OCCURRENCE.name)).label('OCCURRENCE_OF_TARGET_VARIANT')
        func_frequency = func.rr.mut_frequency(func_count_occurrence, func_count_donors, chromosome).label(Vocabulary.FREQUENCY.name)

        # merge results by union (which removes duplicates) and count
        stmt = \
            select(by_attributes_as_columns + [func_count_donors, func_count_positive_donors, func_count_occurrence, func_frequency]) \
            .select_from(union(*from_sources).alias('all_sources')) \
            .group_by(func.cube(*by_attributes_as_columns))

        def compute_result(connection: Connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, stmt, logger.debug, 'VARIANT DISTRIBUTION')
            result = connection.execute(stmt)
            return result

        return result_proxy_as_dict(database.try_py_function(compute_result)), 200


def most_common_variants(meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, out_max_freq: Optional[float], limit_result: Optional[int] = 10):
    transformed_region_attrs = resolve_gene_name_into_interval(region_attrs)
    if transformed_region_attrs[1] == 200:
        region_attrs = transformed_region_attrs[0]
    else:
        return transformed_region_attrs
    if limit_result is None:
        limit_result = 10
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.most_common_variant)]

    notices = list()

    def ask_to_source(source: Type[Source]):
        def do():
            obj = source()

            def most_common_var_from_source(connection: Connection):
                source_stmt = obj.most_common_variant(connection, meta_attrs, region_attrs, out_max_freq, limit_result)\
                    .alias(source.__name__)
                return \
                    select([
                        column(Vocabulary.CHROM.name),
                        column(Vocabulary.START.name),
                        column(Vocabulary.REF.name),
                        column(Vocabulary.ALT.name),
                        column(Vocabulary.POPULATION_SIZE.name),
                        column(Vocabulary.POSITIVE_DONORS.name),
                        column(Vocabulary.OCCURRENCE.name),
                        column(Vocabulary.FREQUENCY.name)
                    ]) \
                    .select_from(source_stmt)
            return database.try_py_function(most_common_var_from_source)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        stmt = \
            select([
                column(Vocabulary.CHROM.name),
                column(Vocabulary.START.name),
                column(Vocabulary.REF.name),
                column(Vocabulary.ALT.name),
                column(Vocabulary.POPULATION_SIZE.name),
                column(Vocabulary.POSITIVE_DONORS.name),
                column(Vocabulary.OCCURRENCE.name).label('OCCURRENCE_OF_VARIANT'),
                column(Vocabulary.FREQUENCY.name).label('FREQUENCY_OF_VARIANT')
            ]) \
            .select_from(union(*from_sources).alias('all_sources')) \
            .order_by(desc(column(Vocabulary.FREQUENCY.name)), desc(column(Vocabulary.OCCURRENCE.name)))

        def compute_result(connection: Connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, stmt, logger.debug, 'MOST COMMON VARIANTS')
            result = connection.execute(stmt)
            return result

        return result_proxy_as_dict(database.try_py_function(compute_result)), 200


def rarest_variants(meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, out_min_freq: Optional[float], limit_result: Optional[int] = 10):
    transformed_region_attrs = resolve_gene_name_into_interval(region_attrs)
    if transformed_region_attrs[1] == 200:
        region_attrs = transformed_region_attrs[0]
    else:
        return transformed_region_attrs
    if limit_result is None:
        limit_result = 10
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.rarest_variant)]

    notices = list()

    def ask_to_source(source: Type[Source]):
        def do():
            obj = source()

            def rarest_var_from_source(connection: Connection):
                source_stmt = obj.rarest_variant(connection, meta_attrs, region_attrs, out_min_freq, limit_result)\
                    .alias(source.__name__)
                return \
                    select([
                        column(Vocabulary.CHROM.name),
                        column(Vocabulary.START.name),
                        column(Vocabulary.REF.name),
                        column(Vocabulary.ALT.name),
                        column(Vocabulary.POPULATION_SIZE.name),
                        column(Vocabulary.POSITIVE_DONORS.name),
                        column(Vocabulary.OCCURRENCE.name),
                        column(Vocabulary.FREQUENCY.name)
                    ]) \
                    .select_from(source_stmt)

            return database.try_py_function(rarest_var_from_source)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        stmt = \
            select([
                column(Vocabulary.CHROM.name),
                column(Vocabulary.START.name),
                column(Vocabulary.REF.name),
                column(Vocabulary.ALT.name),
                column(Vocabulary.POPULATION_SIZE.name),
                column(Vocabulary.POSITIVE_DONORS.name),
                column(Vocabulary.OCCURRENCE.name).label('OCCURRENCE_OF_VARIANT'),
                column(Vocabulary.FREQUENCY.name).label('FREQUENCY_OF_VARIANT')
            ]) \
            .select_from(union(*from_sources).alias('all_sources')) \
            .order_by(asc(column(Vocabulary.FREQUENCY.name)), asc(column(Vocabulary.OCCURRENCE.name)))

        def compute_result(connection: Connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, stmt, logger.debug, 'RAREST VARIANT')
            result = connection.execute(stmt)
            return result

        return result_proxy_as_dict(database.try_py_function(compute_result)), 200


def values_of_attribute(attribute: Vocabulary):
    eligible_sources = [source for source in _sources if attribute in source.get_available_attributes()]
    eligible_sources.extend([annot_source for annot_source in _annotation_sources if attribute in annot_source.get_available_annotation_types()])

    notices = list()

    def ask_to_source(source):
        def do():
            obj: Source = source()

            def values_from_source(connection: Connection):
                return obj.values_of_attribute(connection, attribute)

            return database.try_py_function(values_from_source)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result]    # removes Nones and empty lists
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        # merge resulting lists and remove duplicates
        return list(set(list(itertools.chain.from_iterable(from_sources)))), 200


def annotate_variant(variant: Mutation, annot_types: List[Vocabulary]):
    region_of_variant = get_region_of_variant(variant)
    if region_of_variant is None:
        error_msg = f'The variant {str(variant)} is not present in our genomic variant sources.'
        logger.debug(error_msg)
        return error_msg, 404
    else:
        return annotate_interval(GenomicInterval(*region_of_variant[0:3], strand=None), annot_types)


def annotate_interval(interval: GenomicInterval, annot_types: List[Vocabulary]):
    which_annotations = set(annot_types)
    eligible_sources = [_source for _source in _annotation_sources
                        if not which_annotations.isdisjoint(_source.get_available_annotation_types())]

    def ask_to_source(source):
        def do():
            obj: AnnotInterface = source()
            available_annot_in_source = obj.get_available_annotation_types()

            select_from_source_output = []  # what we select from the source output (both available and unavailable attributes)
            selectable_attributes: List[Vocabulary] = []  # what we can ask to the source to give us
            for elem in annot_types:
                if elem in available_annot_in_source:
                    selectable_attributes.append(elem)
                    select_from_source_output.append(column(elem.name))
                else:
                    select_from_source_output.append(
                        cast(literal(Vocabulary.unknown.name), types.String).label(elem.name))

            def annotate_region(connection: Connection):
                source_stmt = obj.annotate(connection, interval, selectable_attributes).alias(source.__name__)
                return \
                    select(select_from_source_output)\
                    .select_from(source_stmt)

            return database.try_py_function(annotate_region)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        # aggregate the results of all the queries
        annot_types_as_columns = [column(annot.name) for annot in annot_types]
        stmt = \
            select(['*']) \
            .select_from(union(*from_sources).alias("all_sources")) \
            .order_by(literal(1, types.Integer))

        def compute_result(connection: Connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, stmt, logger.debug, 'ANNOTATE GENOMIC INTERVAL')
            result = connection.execute(stmt)
            return result

        return result_proxy_as_dict(database.try_py_function(compute_result)), 200


def variants_in_gene(gene: Gene):
    select_from_sources = [
        Vocabulary.GENE_TYPE, Vocabulary.CHROM, Vocabulary.START, Vocabulary.STOP, Vocabulary.GENE_ID
    ]
    select_from_sources_as_set = set(select_from_sources)
    eligible_sources = [_source for _source in _annotation_sources
                        if select_from_sources_as_set.issubset(_source.get_available_annotation_types())]

    def ask_to_source(source):
        def do():
            obj: AnnotInterface = source()

            def var_in_gene(connection: Connection) -> FromClause:
                return obj.find_gene_region(connection, gene, select_from_sources)

            return database.try_py_function(var_in_gene)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        merge_regions = \
            select(['*']) \
            .select_from(union(*from_sources).alias('all_annot_sources'))

        def compute_region(connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, merge_regions, logger.debug, 'FIND GENE')
            return connection.execute(merge_regions)
        region_of_gene = database.try_py_function(compute_region)

        result = result_proxy_as_dict(region_of_gene)
        if len(result['rows']) > 1:
            result['error'] = 'Different genes match the entry data. Please provide more details about the gene of interest'
            return result, 300
        elif len(result['rows']) == 0:
            return 'No record in our database corresponds to the given gene parameters.', 404
        else:
            genomic_interval = result['rows'][0]
            return variants_in_genomic_interval(GenomicInterval(genomic_interval[1], genomic_interval[2], genomic_interval[3], strand=None))


def variants_in_genomic_interval(interval: GenomicInterval):
    eligible_sources = _sources
    select_attrs = [Vocabulary.CHROM, Vocabulary.START, Vocabulary.REF, Vocabulary.ALT]

    def ask_to_source(source):
        def do():
            obj: Source = source()

            def variant_in_region(connection: Connection):
                return obj.variants_in_region(connection, interval, select_attrs)

            return database.try_py_function(variant_in_region)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
        from_sources = executor.map(ask_to_source, eligible_sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None]
    if len(from_sources) == 0:
        logger.critical('Sources produced no data')
        return 'Internal server error', 503
    else:
        # no need for select distinct as the union does that already
        stmt = \
            select(['*']) \
            .select_from(union(*from_sources).alias("all_sources")) \
            .order_by(literal(2, types.Integer))

        def compute_result(connection: Connection):
            if LOG_SQL_STATEMENTS:
                db_utils.show_stmt(connection, stmt, logger.debug, 'VARIANTS IN GENOMIC INTERVAL')
            result = connection.execute(stmt)
            return result

        return result_proxy_as_dict(database.try_py_function(compute_result)), 200


#   HELPER METHODS  #
def result_proxy_as_dict(result_proxy):
    return {
            'columns': result_proxy.keys(),
            'rows': [row.values() for row in result_proxy.fetchall()]
        }
    

def get_chromosome_of_variant(variant):
    def get_chrom(connection):
        return KGenomes().get_chrom_of_variant(connection, variant)
    return database.try_py_function(get_chrom)


def get_region_of_variant(var: Mutation) -> Optional[list]:
    # to the me of the future: I suggest you to not generalize this method. If you want to generalize it, then you must
    # take care of merging the results coming from multiple sources. Here instead that problem is avoided because
    # it returns only values that are assumed to be equal in all sources.
    def ask_to_source(source):
        def do():
            obj: Source = source()

            def get_region(connection):
                return obj.get_variant_details(connection, var, [Vocabulary.CHROM, Vocabulary.START, Vocabulary.STOP])
            return database.try_py_function(get_region)
        return try_and_catch(do, None)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(_sources)) as executor:
        from_sources = executor.map(ask_to_source, _sources)

    # remove failures
    from_sources = [result for result in from_sources if result is not None and len(result) > 0]
    if len(from_sources) == 0:
        logger.debug('Sources produced no data')
        return None
    else:
        # in case multiple sources have the searched variant, the strategy is to take the first result, assuming that
        # they're equivalent
        return from_sources[0]


def resolve_gene_name_into_interval(region_attr: RegionAttrs):
    gene = region_attr.with_variants_in_gene
    if gene is None:
        return region_attr, 200
    else:
        select_from_sources = [
            Vocabulary.GENE_TYPE, Vocabulary.CHROM, Vocabulary.START, Vocabulary.STOP, Vocabulary.GENE_ID
        ]
        select_from_sources_as_set = set(select_from_sources)
        eligible_sources = [_source for _source in _annotation_sources
                            if select_from_sources_as_set.issubset(_source.get_available_annotation_types())]

        def ask_to_source(source):
            def do():
                obj: AnnotInterface = source()

                def var_in_gene(connection: Connection) -> FromClause:
                    return obj.find_gene_region(connection, gene, select_from_sources)

                return database.try_py_function(var_in_gene)
            return try_and_catch(do, None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)

        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            logger.critical('Sources produced no data')
            return 'Internal server error', 503
        else:
            merge_regions = \
                select(['*']) \
                .select_from(union(*from_sources).alias('all_annot_sources'))

            def compute_region(connection):
                if LOG_SQL_STATEMENTS:
                    db_utils.show_stmt(connection, merge_regions, logger.debug, 'FIND GENE')
                return connection.execute(merge_regions)
            region_of_gene = database.try_py_function(compute_region)

            result = result_proxy_as_dict(region_of_gene)
            if len(result['rows']) > 1:
                result['error'] = 'Different genes match the entry data. Please provide more details about the gene of interest'
                return result, 300
            elif len(result['rows']) == 0:
                return 'No record in our database corresponds to the given gene parameters.', 404
            else:
                genomic_interval = result['rows'][0]
                interval = GenomicInterval(genomic_interval[1], genomic_interval[2], genomic_interval[3], strand=None)
                region_attr.with_variants_in_gene = None
                region_attr.with_variants_in_reg = interval
                return region_attr, 200


def try_and_catch(fun, alternative_return_value, *args, **kwargs):
    try:
        return fun(*args, **kwargs)
    except sqlalchemy.exc.OperationalError as e:  # database connection not available / user canceled query
        logger.exception('database connection not available / user canceled query', e)
        return alternative_return_value
    except sqlalchemy.exc.DatabaseError as e:  # pooled database connection has been invalidated/restarted
        logger.exception('Connection from pool is stale. This error should be already handled in module database.py', e)
        return alternative_return_value
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.exception('Exception occurred in sqlalchemy library. Check your implementation', e)
        return alternative_return_value
    except sqlalchemy.exc.DBAPIError as e:
        logger.exception('Exception from the underlying database', e)
        return alternative_return_value
    except Exception as e:
        logger.exception(e)
        return alternative_return_value
