from data_sources.source_interface import *
from data_sources.kgenomes.kgenomes import KGenomes
from typing import List, Type
from sqlalchemy.engine import ResultProxy
from sqlalchemy import select, union, func, literal, column, cast, types, desc, asc
import sqlalchemy.exc
import database.db_utils as db_utils
import database.database as database
import concurrent.futures
from loguru import logger
import itertools


_sources: List[Type[Source]] = [
    KGenomes
]

LOG_SQL_STATEMENTS = True


def donor_distribution(by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs) -> Optional[ResultProxy]:
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.donors)]

    # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
    by_attributes_copy = by_attributes.copy()
    if Vocabulary.DONOR_ID not in by_attributes_copy:
        by_attributes_copy.append(Vocabulary.DONOR_ID)
    by_attributes_copy.sort(key=lambda x: x.name)

    source_fatal_errors = dict()

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
        return None
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

        return database.try_py_function(compute_result)


def variant_distribution(by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, variant: Mutation) -> Optional[ResultProxy]:
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.variant_occurrence)]

    # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
    by_attributes_copy = by_attributes.copy()
    if Vocabulary.DONOR_ID not in by_attributes_copy:
        by_attributes_copy.append(Vocabulary.DONOR_ID)
    by_attributes_copy.sort(key=lambda x: x.name)

    source_fatal_errors = dict()

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
        return None
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

        return database.try_py_function(compute_result)


def most_common_variants(meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, out_max_freq: float = None, limit_result: int = 10) -> Optional[ResultProxy]:
    if limit_result is None:
        limit_result = 10
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.most_common_variant)]

    source_fatal_errors = dict()

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
        return None
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

        return database.try_py_function(compute_result)


def rarest_variants(meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, out_min_freq: float, limit_result: int = 10) -> Optional[ResultProxy]:
    if limit_result is None:
        limit_result = 10
    eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.rarest_variant)]

    source_fatal_errors = dict()

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
        return None
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

        return database.try_py_function(compute_result)


def values_of_attribute(attribute: Vocabulary) -> Optional[List]:
    eligible_sources = [source for source in _sources if attribute in source.get_available_attributes()]

    source_fatal_errors = dict()

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
        return None
    else:
        return list(itertools.chain.from_iterable(from_sources))


def get_chromosome_of_variant(variant):
    def get_chrom(connection):
        return KGenomes().get_chrom_of_variant(connection, variant)
    return database.try_py_function(get_chrom)


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
