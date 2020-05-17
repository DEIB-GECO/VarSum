from data_sources.annot_interface import AnnotInterface
from data_sources.source_interface import *
from data_sources.kgenomes.kgenomes import KGenomes
from data_sources.tcga.tcga import TCGA
from data_sources.gencode_v19_hg19.gencode import Gencode
from typing import List, Type, Union, Iterable, Sequence
from sqlalchemy.engine import ResultProxy
from sqlalchemy import select, union, func, literal, column, cast, types, desc, asc
import sqlalchemy.exc
import database.database as database
import concurrent.futures
import itertools
import collections


def default_user_callback(*msg) -> None:
    return


gen_var_sources = {
    '1000Genomes': KGenomes,
    'TCGA': TCGA
}
# gen_var_sources: Sequence[Type[Source]] = (
#     KGenomes, TCGA
# )
_annotation_sources: Iterable[Type[AnnotInterface]] = [
    Gencode
]

LOG_SQL_STATEMENTS = True


class Coordinator:
    def __init__(self, request_logger, filter_sources: Optional[Sequence[str]] = None, observer: Callable[[str], None] = default_user_callback):
        self.logger = request_logger
        self.notices = collections.deque()
        self.use_sources = [gen_var_sources[name] for name in filter_sources] or gen_var_sources.values() if filter_sources else gen_var_sources.values()
        self.observer_callback = observer

    def donor_distribution(self, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs,
                           with_download_url: bool) -> dict:
        region_attrs = self.replace_gene_with_interval(region_attrs, meta_attrs.assembly)
        eligible_sources = [source for source in self.use_sources if source.can_express_constraint(meta_attrs, region_attrs, source.donors)]
        answer_204_if_no_source_can_answer(eligible_sources)
        self.warn_if_mixed_germline_somatic_vars(eligible_sources)
    
        # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
        by_attributes_copy = by_attributes.copy()
        if Vocabulary.DONOR_ID not in by_attributes_copy:
            by_attributes_copy.append(Vocabulary.DONOR_ID)
        by_attributes_copy.sort(key=lambda x: x.name)
    
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
            return self.try_catch_source_errors(do, None)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
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

            result = self.get_as_dictionary(stmt, 'DONOR DISTRIBUTION')
            if with_download_url:
                rows = result['rows']
                for row in rows:
                    # here "row" is the string concatenation of all the sample urls in this group
                    row[-1] = row[-1].replace("www.gmql.eu", "genomic.deib.polimi.it")
                    row[-1] = row[-1].replace('.gdm', '')
            return result

    def variant_distribution(self, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, variant: Mutation) -> dict:
        region_attrs = self.replace_gene_with_interval(region_attrs, meta_attrs.assembly)
        eligible_sources = [source for source in self.use_sources if source.can_express_constraint(meta_attrs, region_attrs, source.variant_occurrence)]
        answer_204_if_no_source_can_answer(eligible_sources)
        self.warn_if_mixed_germline_somatic_vars(eligible_sources)
    
        # sorted copy of ( by_attributes + donor_id ) 'cos we need the same table schema from each source
        by_attributes_copy = set(by_attributes)
        by_attributes_copy.update([Vocabulary.DONOR_ID, Vocabulary.GENDER])
        by_attributes_copy = list(by_attributes_copy)
        by_attributes_copy.sort(key=lambda x: x.name)
    
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
            return self.try_catch_source_errors(do, None)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources) + 1) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
            if variant.chrom is None:
                region_of_variant = executor.submit(self.get_region_of_variant, variant, meta_attrs.assembly).result()
            else:
                region_of_variant = [variant.chrom, variant.start, variant.start+1]  # stop is fake but I don't need it anyway
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
        else:
            all_sources = union(*from_sources).alias('all_sources')
            chrom = region_of_variant[0]
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
                self.notices.append(Notice('The target variant is located in a non-autosomal chromosome, as such the '
                                           'individuals of the selected population having unknown gender have been excluded '
                                           'from the frequency computation.'))
                stmt = stmt.where(column(Vocabulary.GENDER.name).in_(['male', 'female']))
            stmt = stmt.group_by(func.cube(*by_attributes_as_columns))
    
            return self.get_as_dictionary(stmt, 'VARIANT DISTRIBUTION')

    def rank_variants_by_freq(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, ascending: bool,
                              out_min_freq: Optional[float], limit_result: Optional[int] = 10) -> dict:
        region_attrs = self.replace_gene_with_interval(region_attrs, meta_attrs.assembly)
        limit_result = limit_result or 10
        eligible_sources = [source for source in self.use_sources if
                            source.can_express_constraint(meta_attrs, region_attrs, source.rank_variants_by_frequency)]
        answer_204_if_no_source_can_answer(eligible_sources)
        self.warn_if_mixed_germline_somatic_vars(eligible_sources)
    
        def ask_to_source(source: Type[Source]):
            def do():
                obj: Source = source(self.logger, self.source_message_handler)
    
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
            return self.try_catch_source_errors(do, None)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
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
            return self.get_as_dictionary(stmt, 'RANKED VARIANTS {}'.format('ASC' if ascending else 'DESC'))
    
    def values_of_attribute(self, attribute: Vocabulary) -> dict:
        eligible_sources = [source for source in self.use_sources if attribute in source.get_available_attributes()]
        eligible_sources.extend([annot_source for annot_source in _annotation_sources if attribute in annot_source.get_available_annotation_types()])
        answer_204_if_no_source_can_answer(eligible_sources)
    
        def ask_to_source(source: Union[Type[Source], Type[AnnotInterface]]):
            def do():
                obj = source(self.logger)
    
                def values_from_source(connection: Connection):
                    return obj.values_of_attribute(connection, attribute)
    
                return database.try_py_function(values_from_source)
            return self.try_catch_source_errors(do, None)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result]    # removes Nones and empty lists
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
        else:
            only_values = [res[1] for res in from_sources]  # list of lists of values
            unique_values = list(set(list(itertools.chain.from_iterable(only_values))))  # list of values

            def find_in_source(val: str) -> list:
                find_in = []
                for source_name_and_values in from_sources:
                    if val in source_name_and_values[1]:
                        find_in.append(source_name_and_values[0])
                return find_in

            result = {value: find_in_source(value) for value in unique_values}
            if self.notices:
                result['notice'] = [notice.args for notice in self.notices]
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
            return self.try_catch_source_errors(do, None)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
        else:
            # aggregate the results of all the queries
            stmt = \
                select(['*']) \
                .select_from(union(*from_sources).alias("all_sources")) \
                .order_by(literal(1, types.Integer))
    
            return self.get_as_dictionary(stmt, 'ANNOTATE GENOMIC INTERVAL')

    def variants_in_gene(self, gene: Gene, meta_attrs: MetadataAttrs, region_attrs: Optional[RegionAttrs]) -> dict:
        genomic_interval = self.resolve_gene_interval(gene, meta_attrs.assembly)
        return self.variants_in_genomic_interval(genomic_interval, meta_attrs, region_attrs)

    def variants_in_genomic_interval(self, interval: GenomicInterval, meta_attrs: MetadataAttrs, region_attrs: Optional[RegionAttrs]) -> dict:
        eligible_sources = [source for source in self.use_sources if source.can_express_constraint(meta_attrs, region_attrs, source.variants_in_region)]
        answer_204_if_no_source_can_answer(eligible_sources)
        self.warn_if_mixed_germline_somatic_vars(eligible_sources)
        select_attrs = [Vocabulary.CHROM, Vocabulary.START, Vocabulary.REF, Vocabulary.ALT]
    
        def ask_to_source(source):
            def do():
                obj: Source = source(self.logger)
    
                def variant_in_region(connection: Connection):
                    return obj.variants_in_region(connection, interval, select_attrs, meta_attrs, region_attrs)
    
                return database.try_py_function(variant_in_region)
            return self.try_catch_source_errors(do, None)
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)
    
        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
        else:
            # no need for select distinct as the union does that already
            stmt = \
                select(['*']) \
                .select_from(union(*from_sources).alias("all_sources")) \
                .order_by(literal(2, types.Integer))
    
            return self.get_as_dictionary(stmt, 'VARIANTS IN GENOMIC INTERVAL')

    #   HELPER METHODS  #
    def get_region_of_variant(self, variant: Mutation, assembly: str):
        """Returns an array of values corresponding to CHROM, START, STOP of this variant.
        """

        def ask_to_source(source):
            def do():
                obj: Source = source(self.logger)

                def get_region(connection):
                    return obj.get_variant_details(connection, variant,
                                                   [Vocabulary.CHROM, Vocabulary.START, Vocabulary.STOP], assembly)

                return database.try_py_function(get_region)

            return self.try_catch_source_errors(do, None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.use_sources)) as executor:
            from_sources = executor.map(ask_to_source, self.use_sources)

        # remove failures
        from_sources = [result for result in from_sources if result is not None and len(result) > 0]
        if len(from_sources) == 0:
            self.logger.debug(f'It\'s unclear if sources are not available or variant {variant} cannot be located. '
                              f'AskUserIntervention with 404 raised anyway.')
            body = {
                'error': f'The variant {variant} is not present in our genomic variant sources.'
            }
            if self.notices:
                body['notice'] = self.notices
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

        def ask_to_source(source):
            def do():
                obj: AnnotInterface = source(self.logger)

                def var_in_gene(connection: Connection) -> FromClause:
                    return obj.find_gene_region(connection, gene, select_from_sources, assembly)

                return database.try_py_function(var_in_gene)
            return self.try_catch_source_errors(do, None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(eligible_sources)) as executor:
            from_sources = executor.map(ask_to_source, eligible_sources)

        # remove failures
        from_sources = [result for result in from_sources if result is not None]
        if len(from_sources) == 0:
            raise NoDataFromSources(self.notices)
        else:
            merge_regions = \
                select(['*']) \
                .select_from(union(*from_sources).alias('all_annot_sources'))

            result = self.get_as_dictionary(merge_regions, 'FIND GENE')
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

    def try_catch_source_errors(self, fun, alternative_return_value):
        # noinspection PyBroadException
        try:
            return fun()
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
        except EmptyResult as empty_res:
            try:
                self.logger.debug(f'A source returned prematurely with empty result: '
                                  f'{empty_res.args[0]}')
                self.notices.append(Notice(empty_res.args[0]))
            except IndexError:
                self.logger.error('a source returned prematurely with empty result. Forget to pass the name of the '
                                  'source into the exception EmptyResult')
            finally:
                return alternative_return_value
        except Notice as notice:
            # notices are eventually added to the response if the response is still a valid response,
            # or attached to a more severe exception otherwise. So they will be part of the result in any case.
            self.logger.info(notice.msg)
            self.notices.append(notice)
            return alternative_return_value
        except Exception:
            self.logger.exception('unknown exception caught from a source')
            return alternative_return_value

    def get_as_dictionary(self, stmt_to_execute, log_with_intro: Optional[str]):
        log_fun = self.logger.debug if LOG_SQL_STATEMENTS else None
        result_proxy: ResultProxy = database.try_stmt(stmt_to_execute, log_fun, log_with_intro)
        result = {
            'columns': result_proxy.keys(),
            'rows': [row.values() for row in result_proxy.fetchall()]
        }
        if self.notices:
            result['notice'] = [notice.args[0] for notice in self.notices]
        return result

    def source_message_handler(self, msg_type: SourceMessage.Type, msg: str):
        if msg_type == SourceMessage.Type.TIME_TO_FINISH:
            self.observer_callback(msg)
        else:
            self.notices.append(Notice(msg))

    def warn_if_mixed_germline_somatic_vars(self, eligible_sources: Iterable[Type[Source]]):
        cell_types = set()
        for s in eligible_sources:
            if Vocabulary.WITH_VARIANTS_IN_SOMATIC_CELLS in s.avail_region_constraints:
                cell_types.add(Vocabulary.WITH_VARIANTS_IN_SOMATIC_CELLS)
            if Vocabulary.WITH_VARIANTS_IN_GERMLINE_CELLS in s.avail_region_constraints:
                cell_types.add(Vocabulary.WITH_VARIANTS_IN_GERMLINE_CELLS)
        if Vocabulary.WITH_VARIANTS_IN_GERMLINE_CELLS in cell_types and Vocabulary.WITH_VARIANTS_IN_SOMATIC_CELLS in cell_types:
            self.notices.append(Notice("This response may contain data from mixed germline and somatic mutations data "
                                       "sources. If that's not the desired output, you can constrain the cell "
                                       "type with the parameter having_variants -> in_cell_type."))


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

    def __init__(self, notices=None):
        super().__init__(notices)
        if notices:
            self.response_body = {
                'notice': [notice.args[0] for notice in notices]
            }
