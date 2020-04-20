from data_sources.annot_interface import AnnotInterface
from data_sources.source_interface import *
from data_sources.kgenomes.kgenomes import KGenomes
from data_sources.tcga.tcga import TCGA
from data_sources.gencode_v19_hg19.gencode import Gencode
from typing import List, Type
from sqlalchemy.engine import ResultProxy
from sqlalchemy import select, union, func, literal, column, cast, types, desc, asc
import sqlalchemy.exc
import database.database as database
import concurrent.futures
import itertools
import warnings


_sources: List[Type[Source]] = [
    TCGA
]
_annotation_sources: List[Type[AnnotInterface]] = [
    Gencode
]

LOG_SQL_STATEMENTS = True


class Coordinator:
    def __init__(self, request_logger):
        self.logger = request_logger

    def donor_distribution(self, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs,
                           with_download_url: bool) -> dict:
        region_attrs = self.replace_gene_with_interval(region_attrs, meta_attrs.assembly)
        eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.donors)]
        answer_204_if_no_source_can_answer(eligible_sources)
    
        # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
        by_attributes_copy = by_attributes.copy()
        if Vocabulary.DONOR_ID not in by_attributes_copy:
            by_attributes_copy.append(Vocabulary.DONOR_ID)
        by_attributes_copy.sort(key=lambda x: x.name)
    
        notices = list()
    
        # collect results from individual sources
        def ask_to_source(source: Type[Source]):
            def do():
                obj: Source = source(self.logger)
                available_attributes_in_source = obj.get_available_attributes()
    
                select_from_source_output = []  # what we select from the source output (both available and unavailable attributes)
                selectable_attributes: List[Vocabulary] = []  # what we can ask to the source to give us
                for elem in by_attributes_copy:
                    if elem in available_attributes_in_source:
                        selectable_attributes.append(elem)
                        select_from_source_output.append(column(elem.name))
                    else:
                        select_from_source_output.append(cast(literal(Vocabulary.unknown.name), types.String).label(elem.name))

                if with_download_url:
                    select_from_source_output.append(column(Vocabulary.DOWNLOAD_URL.name))
    
                def donors(a_connection):
                    source_stmt = obj.donors(a_connection, selectable_attributes, meta_attrs, region_attrs, with_download_url)\
                        .alias(source.__name__)
                    return \
                        select(select_from_source_output) \
                        .select_from(source_stmt)
    
                return database.try_py_function(donors)
            return self.try_catch_source_errors(do, None, notices)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
        else:
            # aggregate the results of all the queries
            by_attributes_as_columns = [column(att.name) for att in by_attributes]
            if with_download_url:
                download_col = [func.string_agg(
                    func.concat(column(Vocabulary.DOWNLOAD_URL.name), '?authToken=DOWNLOAD-TOKEN'),
                    ', ').label(Vocabulary.DOWNLOAD_URL.name)]
            else:
                download_col = []
            stmt = \
                select(
                    by_attributes_as_columns +
                    [func.count(column(Vocabulary.DONOR_ID.name)).label('DONORS')] +
                    download_col
                )\
                .select_from(union(*from_sources).alias("all_sources"))\
                .group_by(func.cube(*by_attributes_as_columns))

            result = self.get_as_dictionary(stmt, 'DONOR DISTRIBUTION', notices)
            if with_download_url:
                rows = result['rows']
                for row in rows:
                    # here "row" is the string concatenation of all the sample urls in this group
                    row[-1] = row[-1].replace("www.gmql.eu", "genomic.deib.polimi.it")
                    row[-1] = row[-1].replace('.gdm', '')
            return result

    def variant_distribution(self, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, variant: Mutation) -> dict:
        region_attrs = self.replace_gene_with_interval(region_attrs, meta_attrs.assembly)
        eligible_sources = [source for source in _sources if source.can_express_constraint(meta_attrs, region_attrs, source.variant_occurrence)]
        answer_204_if_no_source_can_answer(eligible_sources)
    
        # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
        by_attributes_copy = set(by_attributes)
        by_attributes_copy.update([Vocabulary.DONOR_ID, Vocabulary.GENDER])
        by_attributes_copy = list(by_attributes_copy)
        by_attributes_copy.sort(key=lambda x: x.name)
    
        notices = list()
    
        # collect results from individual sources as DONOR_ID | OCCURRENCE | <by_attributes>
        def ask_to_source(source: Type[Source]):
            def do():
                obj: Source = source(self.logger)
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
            return self.try_catch_source_errors(do, None, notices)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources) + 1) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
            if variant.chrom is None:
                region_of_variant = executor.submit(self.get_region_of_variant, variant, meta_attrs.assembly).result()
            else:
                region_of_variant = [variant.chrom, variant.start, variant.start+1]  # stop is fake but I don't need it anyway
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
        else:
            all_sources = union(*from_sources).alias('all_sources')
            chrom = int(region_of_variant[0])
            start = region_of_variant[1]

            # functions
            func_count_donors = func.count(column(Vocabulary.DONOR_ID.name)).label('POPULATION_SIZE')
            # in the following statements 1 is an abbreviation for the column DONOR_ID
            func_count_positive_donors = func.count(1).filter(column(Vocabulary.OCCURRENCE.name) > 0).label('POSITIVE_DONORS')
            func_count_males_and_na = cast(func.count(1).filter(func.coalesce(column(Vocabulary.GENDER.name), '') != 'female'), types.Integer)
            func_count_females = cast(func.count(1).filter(column(Vocabulary.GENDER.name) == 'female'), types.Integer)
            func_count_occurrence = func.sum(column(Vocabulary.OCCURRENCE.name)).label('OCCURRENCE_OF_TARGET_VARIANT')
            if meta_attrs.assembly == 'hg19':
                func_frequency_new = func.rr.mut_frequency_new_hg19(func_count_occurrence, func_count_males_and_na,
                                                                    func_count_females, chrom, start)
            else:
                func_frequency_new = func.rr.mut_frequency_new_grch38(func_count_occurrence, func_count_males_and_na,
                                                                      func_count_females, chrom, start)
            func_frequency_new = func_frequency_new.label(Vocabulary.FREQUENCY.name)
    
            # merge results by union (which removes duplicates) and count
            by_attributes_as_columns = [column(att.name) for att in by_attributes]
            stmt = \
                select(by_attributes_as_columns + [func_count_donors, func_count_positive_donors, func_count_occurrence, func_frequency_new]) \
                .select_from(all_sources)
            if chrom == 23 or chrom == 24:
                notices.append(Notice('The target variant is located in a non-autosomal chromosome, as such the '
                                      'individuals of the selected population having unknown gender have been excluded '
                                      'from the frequency computation.'))
                stmt = stmt.where(column(Vocabulary.GENDER.name).in_(['male', 'female']))
            stmt = stmt.group_by(func.cube(*by_attributes_as_columns))
    
            return self.get_as_dictionary(stmt, 'VARIANT DISTRIBUTION', notices)

    def rank_variants_by_freq(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, ascending: bool,
                              out_min_freq: Optional[float], limit_result: Optional[int] = 10) -> dict:
        region_attrs = self.replace_gene_with_interval(region_attrs, meta_attrs.assembly)
        limit_result = limit_result or 10
        eligible_sources = [source for source in _sources if
                            source.can_express_constraint(meta_attrs, region_attrs, source.rank_variants_by_frequency)]
        answer_204_if_no_source_can_answer(eligible_sources)
        notices = list()
    
        def ask_to_source(source: Type[Source]):
            def do():
                obj: Source = source(self.logger)
    
                def rank_var(connection: Connection):
                    source_stmt = obj.rank_variants_by_frequency(connection, meta_attrs, region_attrs, ascending,
                                                                 out_min_freq, limit_result)\
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
    
                return database.try_py_function(rank_var)
            return self.try_catch_source_errors(do, None, notices)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
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
                .select_from(union(*from_sources).alias('all_sources'))
            if ascending:
                stmt = stmt.order_by(asc(column(Vocabulary.FREQUENCY.name)), asc(column(Vocabulary.OCCURRENCE.name)))
            else:
                stmt = stmt.order_by(desc(column(Vocabulary.FREQUENCY.name)), desc(column(Vocabulary.OCCURRENCE.name)))
            return self.get_as_dictionary(stmt, 'RANKED VARIANTS {}'.format('ASC' if ascending else 'DESC'), notices)
    
    def values_of_attribute(self, attribute: Vocabulary) -> dict:
        eligible_sources = [source for source in _sources if attribute in source.get_available_attributes()]
        eligible_sources.extend([annot_source for annot_source in _annotation_sources if attribute in annot_source.get_available_annotation_types()])
        answer_204_if_no_source_can_answer(eligible_sources)

        notices = list()
    
        def ask_to_source(source):
            def do():
                obj = source(self.logger)
    
                def values_from_source(connection: Connection):
                    return obj.values_of_attribute(connection, attribute)
    
                return database.try_py_function(values_from_source)
            return self.try_catch_source_errors(do, None, notices)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result]    # removes Nones and empty lists
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
        else:
            result = {
                # merge resulting lists and remove duplicates
                'values': list(set(list(itertools.chain.from_iterable(from_sources))))
            }
            if notices:
                result['notice'] = [notice.args for notice in notices]
            return result

    def annotate_variant(self, variant: Mutation, assembly: str) -> dict:
        region_of_variant = self.get_region_of_variant(variant, assembly)
        return self.annotate_interval(GenomicInterval(*region_of_variant[0:3], strand=None), assembly)

    def annotate_interval(self, interval: GenomicInterval, assembly: str) -> dict:
        annot_types = [
            Vocabulary.CHROM,
            Vocabulary.START,
            Vocabulary.STOP,
            Vocabulary.STRAND,
            Vocabulary.GENE_NAME,
            Vocabulary.GENE_TYPE
        ]
        which_annotations = set(annot_types)
        eligible_sources = [_source for _source in _annotation_sources
                            if not which_annotations.isdisjoint(_source.get_available_annotation_types())]
        answer_204_if_no_source_can_answer(eligible_sources)
    
        notices = list()
    
        def ask_to_source(source):
            def do():
                obj: AnnotInterface = source(self.logger)
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
                    source_stmt = obj.annotate(connection, interval, selectable_attributes, assembly).alias(source.__name__)
                    return \
                        select(select_from_source_output)\
                        .select_from(source_stmt)
    
                return database.try_py_function(annotate_region)
            return self.try_catch_source_errors(do, None, notices)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
        else:
            # aggregate the results of all the queries
            stmt = \
                select(['*']) \
                .select_from(union(*from_sources).alias("all_sources")) \
                .order_by(literal(1, types.Integer))
    
            return self.get_as_dictionary(stmt, 'ANNOTATE GENOMIC INTERVAL', notices)

    def variants_in_gene(self, gene: Gene, assembly: str) -> dict:
        genomic_interval = self.resolve_gene_interval(gene, assembly)
        return self.variants_in_genomic_interval(genomic_interval, assembly)

    def variants_in_genomic_interval(self, interval: GenomicInterval, assembly: str) -> dict:
        eligible_sources = _sources
        select_attrs = [Vocabulary.CHROM, Vocabulary.START, Vocabulary.REF, Vocabulary.ALT]
    
        notices = list()
    
        def ask_to_source(source):
            def do():
                obj: Source = source(self.logger)
    
                def variant_in_region(connection: Connection):
                    return obj.variants_in_region(connection, interval, select_attrs, assembly)
    
                return database.try_py_function(variant_in_region)
            return self.try_catch_source_errors(do, None, notices)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
        else:
            # no need for select distinct as the union does that already
            stmt = \
                select(['*']) \
                .select_from(union(*from_sources).alias("all_sources")) \
                .order_by(literal(2, types.Integer))
    
            return self.get_as_dictionary(stmt, 'VARIANTS IN GENOMIC INTERVAL', notices)

    #   HELPER METHODS  #
    def get_region_of_variant(self, variant: Mutation, assembly: str):
        """Returns an array of values corresponding to CHROM, START, STOP of this variant.
        """
        notices = list()

        def ask_to_source(source):
            def do():
                obj: Source = source(self.logger)

                def get_region(connection):
                    return obj.get_variant_details(connection, variant,
                                                   [Vocabulary.CHROM, Vocabulary.START, Vocabulary.STOP], assembly)

                return database.try_py_function(get_region)

            return self.try_catch_source_errors(do, None, notices)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(_sources)) as executor:
            from_sources = executor.map(ask_to_source, _sources)

        # remove failures
        from_sources = [result for result in from_sources if result is not None and len(result) > 0]
        if len(from_sources) == 0:
            self.logger.debug(f'It\'s unclear if sources are not available or variant {variant} cannot be located. '
                              f'AskUserIntervention with 404 raised anyway.')
            body = {
                'error': f'The variant {variant} is not present in our genomic variant sources.'
            }
            if notices:
                body['notice'] = notices
            raise AskUserIntervention(body, 404)
        else:
            # in case multiple sources have the searched variant, the strategy is to take the first result, assuming that
            # they're equivalent
            return from_sources[0]

    def replace_gene_with_interval(self, region_attr: Optional[RegionAttrs], assembly) -> RegionAttrs:
        if region_attr is not None and region_attr.with_variants_in_gene is not None:
            region_attr.with_variants_in_reg = self.resolve_gene_interval(region_attr.with_variants_in_gene, assembly)
            region_attr.with_variants_in_gene = None
        return region_attr

    def resolve_gene_interval(self, gene: Gene, assembly) -> GenomicInterval:
        select_from_sources = [
            Vocabulary.GENE_TYPE, Vocabulary.CHROM, Vocabulary.START, Vocabulary.STOP, Vocabulary.GENE_ID
        ]
        select_from_sources_as_set = set(select_from_sources)
        eligible_sources = [_source for _source in _annotation_sources
                            if select_from_sources_as_set.issubset(_source.get_available_annotation_types())]
        answer_204_if_no_source_can_answer(eligible_sources)

        notices = list()

        def ask_to_source(source):
            def do():
                obj: AnnotInterface = source(self.logger)

                def var_in_gene(connection: Connection) -> FromClause:
                    return obj.find_gene_region(connection, gene, select_from_sources, assembly)

                return database.try_py_function(var_in_gene)
            return self.try_catch_source_errors(do, None, notices)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)

        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(notices)
        else:
            merge_regions = \
                select(['*']) \
                .select_from(union(*from_sources).alias('all_annot_sources'))

            result = self.get_as_dictionary(merge_regions, 'FIND GENE', notices)
            if len(result['rows']) > 1:
                result['error'] = 'Different genes match the entry data. Please provide more details about the gene of interest'
                raise AskUserIntervention(result, 300)
            elif len(result['rows']) == 0:
                raise AskUserIntervention('No record in our database corresponds to the given gene parameters.', 404)
            else:
                genomic_interval = result['rows'][0]
                return GenomicInterval(genomic_interval[1], genomic_interval[2], genomic_interval[3], strand=None)

    # noinspection PyMethodMayBeStatic
    def count_males_females(self, selectable_stmt_with_gender):
        females_and_males_stmt = \
            select([column(Vocabulary.GENDER.name), func.count(column(Vocabulary.GENDER.name))]) \
            .select_from(selectable_stmt_with_gender) \
            .group_by(column(Vocabulary.GENDER.name))

        def do_with_connection(connection):
            return [row.values() for row in connection.execute(females_and_males_stmt).fetchall()]

        females_and_males = database.try_py_function(do_with_connection)
        females = next((el[1] for el in females_and_males if el[0] == 'female'), 0)
        males = next((el[1] for el in females_and_males if el[0] == 'male'), 0)
        return males, females

    def try_catch_source_errors(self, fun, alternative_return_value, container_of_notices: List[Notice]):
        # noinspection PyBroadException
        try:
            # call fun but catch warnings
            with warnings.catch_warnings(record=True) as all_warnings:
                fun_result = fun()
                for w in all_warnings:
                    if issubclass(w.category, SourceWarning):
                        container_of_notices.append(Notice(str(w.message)))
                return fun_result
        except sqlalchemy.exc.OperationalError as e:  # database connection not available / user canceled query
            # This exception is not recoverable here, but it subclass the ones below, so I must catch it here and
            # re-raise if I want to let it be handled outside.
            raise e
        except sqlalchemy.exc.DatabaseError:  # pooled database connection has been invalidated/restarted
            self.logger.exception('Connection from pool is stale. This error should be already handled in module '
                                  'database.py. New attempt is being made by disposing the pool')
            # try again
            database.db_engine.dispose()
            # if it fails, there's nothing I can do, the connection won't be available for other modules neither, so
            # let the exception pass through
            return fun()
        except sqlalchemy.exc.SQLAlchemyError:
            self.logger.exception('Exception occurred in sqlalchemy library. Check your implementation')
            return alternative_return_value
        except sqlalchemy.exc.DBAPIError:
            self.logger.exception('Wrong usage of the underlying database')
            return alternative_return_value
        except Notice as notice:
            # notices are eventually added to the response if the response is still a valid response,
            # or attached to a more severe exception otherwise. So they will be part of the result in any case.
            self.logger.info(notice.msg)
            container_of_notices.append(notice)
            return alternative_return_value
        except Exception:
            self.logger.exception('unknown exception caught in coordinator')
            return alternative_return_value

    def get_as_dictionary(self, stmt_to_execute, log_with_intro: Optional[str], add_notices: List[Notice]):
        log_fun = self.logger.debug if LOG_SQL_STATEMENTS else None
        result_proxy: ResultProxy = database.try_stmt(stmt_to_execute, log_fun, log_with_intro)
        result = {
            'columns': result_proxy.keys(),
            'rows': [row.values() for row in result_proxy.fetchall()]
        }
        if add_notices:
            result['notice'] = [notice.args[0] for notice in add_notices]
        return result


def answer_204_if_no_source_can_answer(eligible_sources):
    if len(eligible_sources) == 0:
        raise AskUserIntervention(
            "No data source can satisfy the given request parameters. If possible, try relaxing some constraints.", 422)


class AskUserIntervention(Exception):
    def __init__(self, response_body, proposed_status_code):
        super().__init__(response_body, proposed_status_code)
        self.response_body = response_body
        self.proposed_status_code = proposed_status_code


class NoDataFromSources(Exception):

    response_body = None
    proposed_status_code = 400

    def __init__(self, notices: Optional[list] = None):
        super().__init__(notices)
        if notices:
            self.response_body = {
                'notice': [notice.args[0] for notice in notices]
            }
