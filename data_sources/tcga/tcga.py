from ..source_interface import *
from ..io_parameters import *
from sqlalchemy import MetaData, Table, cast, select, union_all, union, tuple_, func, exists, asc, desc, text, literal, column, types, case, intersect
from sqlalchemy.sql.expression import Selectable
from sqlalchemy.engine import Connection
from functools import reduce
import database.db_utils as utils
import database.database as database
from threading import RLock
from loguru import logger

# SOURCE TABLE PARAMETERS
default_metadata_table_name = 'genomes_metadata_2'
default_metadata_schema_name = 'dw'
default_region_table_name = 'tcga_dnaseq'
default_region_schema_name = 'rr'
default_schema_to_use_name = 'temp'
db_meta: Optional[MetaData] = None
# SOURCE TABLES
initializing_lock = RLock()
metadata: Optional[Table] = None
regions: Optional[Table] = None
public_item: Optional[Table] = None


class TCGA(Source):

    # MAP ATTRIBUTE NAMES TO TABLE COLUMN NAMES (REQUIRED BY INTERFACE)
    meta_col_map = {
        Vocabulary.GENDER: 'gender',
        Vocabulary.ETHNICITY: 'ethnicity',
        Vocabulary.HEALTH_STATUS: 'health_status',
        Vocabulary.ASSEMBLY: 'assembly',
        Vocabulary.DONOR_ID: 'donor_source_id'
    }
    # REGION CONSTRAINTS THAT CAN BE EXPRESSED WITH THIS SOURCE (REQUIRED BY SOURCE)
    avail_region_constraints = {
        Vocabulary.WITH_VARIANT,
        Vocabulary.WITH_VARIANT_IN_GENOMIC_INTERVAL,
        Vocabulary.WITH_VARIANTS_IN_SOMATIC_CELLS
    }
    region_col_map = {
        Vocabulary.CHROM: 'chrom',
        Vocabulary.START: 'start',
        Vocabulary.STOP: 'stop',
        Vocabulary.STRAND: 'strand',
        Vocabulary.REF: 'ref',
        Vocabulary.ALT: 'alt',
        Vocabulary.VAR_TYPE: 'mut_type',
        Vocabulary.ID: 'id'
    }

    log_sql_commands: bool = True
    
    def __init__(self, logger_instance, notify_message=do_not_notify):
        super().__init__(logger_instance, notify_message)
        self.connection: Optional[Connection] = None
        self.init_singleton_tables()
        self.meta_attrs: Optional[MetadataAttrs] = None
        self.region_attrs: Optional[RegionAttrs] = None
        self.my_meta_t: Optional[Table] = None
        self.my_region_t: Optional[Table] = None

    # SOURCE INTERFACE
    def donors(self, connection, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs, region_attrs: RegionAttrs,
               with_download_urls: bool) -> Selectable:
        """
        Assembles a query statement that, when executed, returns a table containing for each individual matching the
        requirements in meta_attrs and region_attrs, the attributes in "by_attributes"
        """
        # init state
        self.connection = connection
        names_columns_of_interest = [self.meta_col_map[attr] for attr in by_attributes]
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(names_columns_of_interest)
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])

        # TCGA has 4 gender classes: males/females/not reported/<no gender at all>. This trick merges null gender with
        # not reported. Otherwise, when coordinator does group by cube(gender) we would get 2 times a null gender.
        columns_of_interest = [self.my_meta_t.c[self.meta_col_map[attr]].label(attr.name) for attr in by_attributes
                               if attr is not Vocabulary.GENDER]
        if Vocabulary.GENDER in by_attributes:
            columns_of_interest.append(
                func.coalesce(self.my_meta_t.c[self.meta_col_map[Vocabulary.GENDER]], 'not reported')
                    .label(Vocabulary.GENDER.name))

        # compute statistics
        if with_download_urls:
            columns_of_interest.append(public_item.c.local_url.label(Vocabulary.DOWNLOAD_URL.name))
        stmt = select(columns_of_interest)
        if self.my_region_t is not None:
            stmt = stmt.where(self.my_meta_t.c.item_id.in_(
                select([self.my_region_t.c.item_id]).distinct()
            ))
        if with_download_urls:
            stmt = stmt.where(self.my_meta_t.c.item_id == public_item.c.item_id)
        if self.log_sql_commands:
            utils.show_stmt(self.connection, stmt, self.logger.debug, 'TCGA: STMT DONORS WITH REQUIRED ATTRIBUTES')
        return stmt

    def variant_occurrence(self, connection: Connection, by_attributes: list, meta_attrs: MetadataAttrs,
                           region_attrs: RegionAttrs, variant: Mutation) -> Selectable:
        """
        Assembles a query statement that, after execution, returns a table containing for each individual matching the
        conditions in region_attrs and meta_attrs, the attributes given in by_attributes and the number of times
        the given "variant" occurs in each individual.
        """
        # init state
        self.connection = connection
        names_columns_of_interest = [self.meta_col_map[attr] for attr in by_attributes]
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(names_columns_of_interest + ['item_id'])
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])

        # select target attributes from table of metadata with meta_attrs
        stmt_sample_set = select([self.my_meta_t.c[self.meta_col_map[attr]] for attr in by_attributes]
                                 + [self.my_meta_t.c.item_id])
        # join with the table of regions with region_attrs
        if self.my_region_t is not None:
            stmt_sample_set = stmt_sample_set.where(self.my_meta_t.c.item_id.in_(
                select([self.my_region_t.c.item_id]).distinct()
            ))
        stmt_sample_set = stmt_sample_set.alias()

        # select individuals with "variant" in table regions and compute the occurrence for each individual
        func_occurrence = (regions.c.al1 + func.coalesce(regions.c.al2, 0)).label(Vocabulary.OCCURRENCE.name)

        stmt_samples_w_var = self._stmt_where_region_is_any_of_mutations(variant,
                                                                         from_table=regions,
                                                                         select_expression=select([regions.c.item_id, func_occurrence])) \
            .alias('samples_w_var')

        # TCGA has 4 gender classes: males/females/not reported/<no gender at all>. This trick merges null gender with
        # not reported. Otherwise, when coordinator does group by cube(gender) we would get 2 times a null gender.
        columns_of_interest = [stmt_sample_set.c[self.meta_col_map[attr]].label(attr.name) for attr in by_attributes
                               if attr is not Vocabulary.GENDER]
        if Vocabulary.GENDER in by_attributes:
            columns_of_interest.append(
                func.coalesce(stmt_sample_set.c[self.meta_col_map[Vocabulary.GENDER]], 'not reported')
                    .label(Vocabulary.GENDER.name))

        # build a query returning individuals in sample_set and for each, the attributes in "by_attributes" + the occurrence
        # of the given variant
        stmt = \
            select(columns_of_interest
                   + [func.coalesce(column(Vocabulary.OCCURRENCE.name), 0).label(Vocabulary.OCCURRENCE.name)]) \
            .select_from(stmt_sample_set.outerjoin(stmt_samples_w_var,
                                                   stmt_sample_set.c.item_id == stmt_samples_w_var.c.item_id))
        # TODO test what happens if sample set is empty and it is anyway used in the left join statement
        if self.log_sql_commands:
            utils.show_stmt(connection, stmt, self.logger.debug, 'TCGA: STMT VARIANT OCCURRENCE')
        return stmt

    def rank_variants_by_frequency(self, connection, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, ascending: bool,
                                   freq_threshold: float, limit_result: int) -> FromClause:
        # init state
        self.connection = connection
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(['item_id', 'gender'])
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])

        females_and_males_stmt = \
            select([self.my_meta_t.c.gender, func.count()]) \
            .where(self.my_meta_t.c.item_id.in_(select([self.my_region_t.c.item_id]))) \
            .group_by(self.my_meta_t.c.gender)
        gender_of_individuals = [row.values() for row in connection.execute(females_and_males_stmt).fetchall()]
        if len(gender_of_individuals) == 0:
            raise EmptyResult('TCGA has no individuals matching the request parameters.')
        females = next((el[1] for el in gender_of_individuals if el[0] == 'female'), 0)
        males = next((el[1] for el in gender_of_individuals if el[0] == 'male'), 0)
        other_genders = reduce(lambda x1, x2: x1+x2, [el[1] for el in gender_of_individuals]) - males - females
        self.logger.debug(f'TCGA: request /rank_variants_by_frequency for a population of {males+females+other_genders} individuals')

        self.notify_message(
            SourceMessage.Type.GENERAL_WARNING,
            'Note for TCGA data: Individuals with an undefined gender have been excluded from the population while '
            'calculating the frequency of variants in chromosomes 23 and 24')

        # reduce size of the join with regions table
        genomes_red = select(
            [regions.c.item_id, regions.c.chrom, regions.c.start, regions.c.ref, regions.c.alt, regions.c.al1,
             regions.c.al2])\
            .alias('variants_few_columns')

        # custom functions
        func_occurrence = (func.sum(genomes_red.c.al1) + func.sum(func.coalesce(genomes_red.c.al2, 0))).label(
            'occurrence_by_gender')
        func_positive_donors = func.count(genomes_red.c.item_id).label('positives_by_gender')

        # Actually, self.my_region_t already contains only the individuals compatible with meta_attrs, but it can contain
        # duplicated item_id. Since we want to join, it's better to remove them.
        sample_set_with_limit = \
            select([self.my_meta_t.c.item_id, self.my_meta_t.c.gender]) \
            .where(self.my_meta_t.c.item_id.in_(
                select([self.my_region_t.c.item_id])
            )) \
            .alias('sample_set')

        # compute occurrence and positive donors by gender, excluding weird chromosomes (e.g. chrGL000205)
        stmt = select([genomes_red.c.chrom,
                       genomes_red.c.start,
                       genomes_red.c.ref,
                       genomes_red.c.alt,
                       func_occurrence,
                       func_positive_donors,
                       sample_set_with_limit.c.gender]) \
            .select_from(genomes_red.join(
                sample_set_with_limit,
                genomes_red.c.item_id == sample_set_with_limit.c.item_id)) \
            .where(genomes_red.c.chrom < str(26)) \
            .group_by(genomes_red.c.chrom, genomes_red.c.start, genomes_red.c.ref, genomes_red.c.alt, sample_set_with_limit.c.gender) \
            .alias('stmt_1')

        # do not count the occurrences and the positiveness of whom who aren't males/females for a variant in chrom 23/24
        outer_stmt = \
            select([stmt.c.chrom, stmt.c.start, stmt.c.ref, stmt.c.alt] +
                   [
                       case([
                           ((stmt.c.chrom < str(23)) | (stmt.c.chrom > str(24)), males+other_genders)
                            ], else_=males).label('males'),
                       cast(func.sum(column('positives_by_gender')), types.INTEGER).label('positives'),
                       cast(func.sum(column('occurrence_by_gender')), types.INTEGER).label('occurrence')
                   ]
                   ) \
            .where((stmt.c.gender.in_(['male', 'female'])) | (stmt.c.chrom < str(23)) | (stmt.c.chrom > str(24))) \
            .group_by(stmt.c.chrom, stmt.c.start, stmt.c.ref, stmt.c.alt) \
            .alias('stmt_2')

        # other custom function
        if meta_attrs.assembly == 'hg19':
            func_frequency_new = func.rr.mut_frequency_new_hg19(column('occurrence'), column('males'), females,
                                                                cast(outer_stmt.c.chrom, types.INTEGER),
                                                                outer_stmt.c.start)
        else:
            func_frequency_new = func.rr.mut_frequency_new_grch38(column('occurrence'), column('males'), females,
                                                                  cast(outer_stmt.c.chrom, types.INTEGER),
                                                                  outer_stmt.c.start)
        func_frequency_new = func_frequency_new.label(Vocabulary.FREQUENCY.name)

        outer_outer_stmt = \
            select([
                outer_stmt.c.chrom.label(Vocabulary.CHROM.name),
                outer_stmt.c.start.label(Vocabulary.START.name),
                outer_stmt.c.ref.label(Vocabulary.REF.name),
                outer_stmt.c.alt.label(Vocabulary.ALT.name),
                (column('males') + females).label(Vocabulary.POPULATION_SIZE.name),
                column('positives').label(Vocabulary.POSITIVE_DONORS.name),
                column('occurrence').label(Vocabulary.OCCURRENCE.name),
                func_frequency_new
            ])
        if ascending:
            if freq_threshold:
                outer_outer_stmt = outer_outer_stmt.where(func_frequency_new >= freq_threshold)
            outer_outer_stmt = outer_outer_stmt.order_by(asc(func_frequency_new), asc(column('occurrence')))
        else:
            if freq_threshold:
                outer_outer_stmt = outer_outer_stmt.where(func_frequency_new <= freq_threshold)
            outer_outer_stmt = outer_outer_stmt.order_by(desc(func_frequency_new), desc(column('occurrence')))
        outer_outer_stmt = outer_outer_stmt.limit(limit_result) \
            .alias('TCGA_ranked')

        if self.log_sql_commands:
            utils.show_stmt(connection, outer_outer_stmt, self.logger.debug, 'TCGA: RANKING VARIANTS IN SAMPLE SET')
        return outer_outer_stmt

    def values_of_attribute(self, connection, attribute: Vocabulary):
        # VIA DATABASE
        # self.connection = connection
        # region_attributes = regions.columns.keys()
        # meta_attributes = metadata.columns.keys()
        # self.logger.debug('regions:', region_attributes)
        # self.logger.debug('meta:', meta_attributes)
        # if attribute in meta_attributes:
        #     stmt = select([metadata.c[attribute]]).distinct()
        # elif attribute in region_attributes:
        #     stmt = select([regions.c[attribute]]).distinct()
        # else:
        #     raise ValueError(
        #         'the given attribute {} is not part of the metadata columns nor region columns'.format(attribute))
        # if self.log_sql_commands:
        #     utils.show_stmt(self.connection, stmt, 'DISTINCT VALUES OF {}'.format(attribute))
        # return self.connection.execute(stmt)

        # HARDCODED
        # since an attribute can also be mut_type which is not indexed, answering takes forever. This is an easy solution
        # considered that the underlying data is updated rarely, we don't need an index on mut_type.
        if attribute == Vocabulary.DNA_SOURCE:
            raise Notice('Unfortunately it is not known the DNA source of the samples coming from TCGA.')
        elif attribute == Vocabulary.SUPER_POPULATION:
            raise Notice('Unfortunately it is not known the super_population of the samples coming from TCGA.')
        elif attribute == Vocabulary.POPULATION:
            raise Notice('Unfortunately it is not known the population of the samples coming from TCGA.')
        distinct_values = {
            self.meta_col_map[Vocabulary.ASSEMBLY]: [
                'hg19',
                'grch38'
            ],
            self.meta_col_map[Vocabulary.GENDER]: [
                'female',
                'male',
                'not reported'
            ],
            self.meta_col_map[Vocabulary.ETHNICITY]: [
                'american indian or alaska native',
                'black or african american',
                'white',
                'native hawaiian or other pacific islander',
                'not reported',
                'asian'
            ],
            self.meta_col_map[Vocabulary.HEALTH_STATUS]: [
                'false'
            ],
            'mut_type': ['SNP', 'DEL', 'INS', 'DNP', 'TNP']
        }
        return 'TCGA', distinct_values.get(self.meta_col_map.get(attribute))

    # SETTERS
    def _set_region_attributes(self, region_attrs: RegionAttrs):
        self.region_attrs = region_attrs
        self.my_region_t = None  # reset my region table

    def _set_meta_attributes(self, meta_attrs: MetadataAttrs):
        self.meta_attrs = meta_attrs
        self.my_meta_t = None  # reset my meta table

    @staticmethod
    def init_singleton_tables():
        global initializing_lock
        global metadata
        global regions
        global public_item
        global db_meta
        if metadata is None or regions is None or public_item is None:
            initializing_lock.acquire(True)
            if metadata is None or regions is None or public_item is None:
                logger.debug('initializing tables for class TCGA')
                # reflect already existing tables (u can access columns as <table>.c.<col_name> or <table>.c['<col_name>'])
                db_meta = MetaData()
                connection = None
                try:
                    connection = database.check_and_get_connection()
                    metadata = Table(default_metadata_table_name,
                                     db_meta,
                                     autoload=True,
                                     autoload_with=connection,
                                     schema=default_metadata_schema_name)
                    regions = Table(default_region_table_name,
                                    db_meta,
                                    autoload=True,
                                    autoload_with=connection,
                                    schema=default_region_schema_name)
                    public_item = Table('item',
                                        db_meta,
                                        autoload=True,
                                        autoload_with=connection,
                                        schema='public')
                finally:
                    initializing_lock.release()
                    if connection is not None:
                        connection.close()
            else:
                logger.debug('Waiting ongoing initialization of tables')
                initializing_lock.release()

    @staticmethod
    def _stmt_where_region_is_any_of_mutations(*mutations: Mutation, from_table, select_expression, only_item_id_in_table: Optional[Table] = None):
        """
        :param mutations:
        :param from_table:
        :param select_expression:
        :param only_item_id_in_table: a table containing a column of item_id. When present, the individuals having any
        of the given mutations is further filtered by considering only the ones in this table.
        :return: the SQL statement querying all the regions from the given source region table, where the regions
        matches one of the given mutations. The returned query selects only the properties given in select_expression.
        """
        mutations_having_id = [mut for mut in mutations if mut.id is not None]
        mutations_without_id = [mut for mut in mutations if mut.id is None]
        first_select, second_select = None, None
        if len(mutations_having_id) > 0:
            first_select = select_expression.where(from_table.c.id.in_([mut.id for mut in mutations_having_id]))
            if only_item_id_in_table is not None:
                first_select = first_select.where(from_table.c.item_id.in_(select([only_item_id_in_table.c.item_id])))
        if len(mutations_without_id) > 0:
            second_select = select_expression.where(
                tuple_(from_table.c.start, from_table.c.ref, from_table.c.alt, from_table.c.chrom).in_(
                    [(mut.start, mut.ref, mut.alt, str(mut.chrom)) for mut in mutations_without_id]
                ))
            if only_item_id_in_table is not None:
                second_select = second_select.where(from_table.c.item_id.in_(select([only_item_id_in_table.c.item_id])))
        if first_select is not None and second_select is not None:
            return union_all(first_select, second_select)
        elif first_select is not None:
            return first_select
        else:
            return second_select

    # GENERATE DB ENTITIES
    def create_table_of_meta(self, select_columns: Optional[list]):
        """Assigns my_meta_t as the table containing only the individuals with the required metadata characteristics"""
        columns_in_select = [metadata]  # take all columns by default
        if select_columns is not None:  # otherwise take the ones in select_columns but make sure item_id is present
            temp_set = set(select_columns)
            temp_set.add('item_id')
            columns_in_select = [metadata.c[col_name] for col_name in temp_set]
        query = select(columns_in_select).where(metadata.c.item_id.in_(
            text("select item_id from public.item where dataset_id in ( "
                 "select dataset_id from public.dataset "
                 "where dataset_name ilike '%TCGA_dnaseq%' "
                 "or dataset_name ilike '%TCGA_somatic_mutation_masked%' "
                 ")")))
        if self.meta_attrs.gender:
            query = query.where(metadata.c.gender == self.meta_attrs.gender)
        if self.meta_attrs.health_status:
            query = query.where(metadata.c.health_status == self.meta_attrs.health_status)
        if self.meta_attrs.assembly:
            query = query.where(metadata.c.assembly == self.meta_attrs.assembly)
        if self.meta_attrs.ethnicity:
            query = query.where(metadata.c.ethnicity.in_(self.meta_attrs.ethnicity))
        new_meta_table_name = utils.random_t_name_w_prefix('meta')
        utils.create_table_as(new_meta_table_name, query, default_schema_to_use_name, self.connection, self.log_sql_commands, self.logger.debug)
        # t_stmt = utils.stmt_create_table_as(new_meta_table_name, query,  default_schema_to_use_name)
        # if self.log_sql_commands:
        #     utils.show_stmt(t_stmt, 'TABLE OF SAMPLES HAVING META')
        # self.connection.execute(t_stmt)
        self.my_meta_t = Table(new_meta_table_name, db_meta, autoload=True, autoload_with=self.connection, schema=default_schema_to_use_name)

    def create_table_of_regions(self, select_columns: Optional[list]):
        if self.region_attrs:
            # compute each filter on regions separately
            to_combine_t = list()
            if self.region_attrs.with_variants:
                t = self.table_with_all_of_mutations(select_columns)
                to_combine_t.append(t)
            if self.region_attrs.with_variants_in_reg:
                t = self.view_of_variants_in_interval_or_type(select_columns)
                to_combine_t.append(t)
            if len(to_combine_t) == 0:
                self.my_region_t = None
            if len(to_combine_t) == 1:  # when only one filter kind
                self.my_region_t = to_combine_t[0]
            elif len(to_combine_t) > 1:
                self.my_region_t = self.take_regions_of_common_individuals(to_combine_t)

    def table_with_all_of_mutations(self, select_columns: Optional[list]):
        """
         Returns a table of variants of the same type of the ones contained in RegionAttrs.with_variants and only form the
         individuals that own all of them.
         :param select_columns: the list of column names to select from the result. If None, all the columns are taken.
        """
        if not self.region_attrs.with_variants:
            raise ValueError('instance parameter self.with_variants not initialized')
        elif len(self.region_attrs.with_variants) == 1:
            return self._table_with_any_of_mutations(select_columns, self.my_meta_t, *self.region_attrs.with_variants)
        else:
            union_select_column_names = None  # means all columns
            if select_columns is not None:  # otherwise use select_columns + minimum necessary
                union_select_column_names = set(select_columns)
                union_select_column_names.add('item_id')
            union_table = self._table_with_any_of_mutations(union_select_column_names, self.my_meta_t,
                                                            *self.region_attrs.with_variants)
            # extracts only the samples having all the mutations
            result_select_columns = [union_table]  # means all columns
            if select_columns is not None:  # otherwise use selected_columns
                result_select_columns = [union_table.c[col_name] for col_name in select_columns]
            stmt_as = \
                select(result_select_columns) \
                .where(union_table.c.item_id.in_(
                    select([union_table.c.item_id])
                    .group_by(union_table.c.item_id)
                    .having(func.count(union_table.c.item_id) == len(self.region_attrs.with_variants))
                ))
            target_t_name = utils.random_t_name_w_prefix('with')
            stmt_create_table = utils.stmt_create_table_as(target_t_name, stmt_as, default_schema_to_use_name)
            if self.log_sql_commands:
                utils.show_stmt(self.connection, stmt_create_table, self.logger.debug,
                                'INDIVIDUALS HAVING "ALL" THE {} MUTATIONS (WITH DUPLICATE ITEM_ID)'.format(
                                    len(self.region_attrs.with_variants)))
            self.connection.execute(stmt_create_table)
            if self.log_sql_commands:
                self.logger.debug('DROP TABLE ' + union_table.name)
            union_table.drop(self.connection)
            return Table(target_t_name, db_meta, autoload=True, autoload_with=self.connection, schema=default_schema_to_use_name)

    def _table_with_any_of_mutations(self, select_columns, only_item_id_in_table: Optional[Table], *mutations: Mutation):
        """Returns a Table containing all the rows from the table regions containing one of the variants in
        the argument mutations.
        :param select_columns selects only the column names in this collection. If None, selects all the columns from regions.
        :param only_item_id_in_table If None, the variants that are not owned by any of the individuals in this table
        are discarded from the result.
        """
        if len(mutations) == 0:
            raise ValueError('function argument *mutations cannot be empty')
        else:
            # create table for the result
            t_name = utils.random_t_name_w_prefix('with_any_of_mut')
            columns = [regions.c[c_name] for c_name in select_columns] if select_columns is not None else [
                regions]
            stmt_as = self._stmt_where_region_is_any_of_mutations(*mutations,
                                                                  from_table=regions,
                                                                  select_expression=select(columns),
                                                                  only_item_id_in_table=only_item_id_in_table)
        stmt_create_table = utils.stmt_create_table_as(t_name, stmt_as, default_schema_to_use_name)
        if self.log_sql_commands:
            utils.show_stmt(self.connection, stmt_create_table, self.logger.debug,
                            'CREATE TABLE HAVING ANY OF THE {} MUTATIONS'.format(len(mutations)))
        self.connection.execute(stmt_create_table)
        return Table(t_name, db_meta, autoload=True, autoload_with=self.connection,
                     schema=default_schema_to_use_name)

    def view_of_variants_in_interval_or_type(self, select_columns: Optional[list]):
        if self.region_attrs.with_variants_in_reg is None and self.region_attrs.with_variants_of_type is None:
            raise ValueError('you called this method without giving any selection criteria')
        columns = [regions.c[c_name] for c_name in select_columns] if select_columns is not None else [regions]
        stmt_as = select(columns)
        if self.region_attrs.with_variants_in_reg is not None:
            stmt_as = stmt_as.where((regions.c.chrom == str(self.region_attrs.with_variants_in_reg.chrom)) &
                                    (regions.c.start >= self.region_attrs.with_variants_in_reg.start) &
                                    (regions.c.start <= self.region_attrs.with_variants_in_reg.stop))
        if self.region_attrs.with_variants_of_type is not None:
            stmt_as = stmt_as.where(regions.c.mut_type.in_(self.region_attrs.with_variants_of_type))
        generated_view_name = utils.random_t_name_w_prefix('mut_of_type_interval')
        stmt = utils.stmt_create_view_as(generated_view_name, stmt_as, default_schema_to_use_name)
        if self.log_sql_commands:
            utils.show_stmt(self.connection, stmt, self.logger.debug,
                            'VIEW OF REGIONS IN INTERVAL {} of types {}'.format(self.region_attrs.with_variants_in_reg,
                                                                                self.region_attrs.with_variants_of_type))
        self.connection.execute(stmt)
        return Table(generated_view_name, db_meta, autoload=True, autoload_with=self.connection,
                     schema=default_schema_to_use_name)

    def variants_in_region(self, connection: Connection, genomic_interval: GenomicInterval,
                           output_region_attrs: List[Vocabulary], meta_attrs: MetadataAttrs,
                           region_attrs: Optional[RegionAttrs]) -> Selectable:
        # init state
        self.connection = connection
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(['item_id'])
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])

        if self.my_region_t is not None:
            only_from_samples = intersect(select([self.my_meta_t.c.item_id]), select([self.my_region_t.c.item_id]))
        else:
            only_from_samples = select([self.my_meta_t.c.item_id])
        only_from_samples = only_from_samples.alias('samples')

        select_columns = list()
        for att in output_region_attrs:
            if att == Vocabulary.CHROM:
                select_columns.append(cast(regions.c.chrom, types.SmallInteger).label(att.name))
            else:
                select_columns.append(regions.c[self.region_col_map[att]].label(att.name))
        stmt =\
            select(select_columns).distinct() \
            .select_from(regions.join(only_from_samples, only_from_samples.c.item_id == regions.c.item_id)) \
            .where((regions.c.start >= genomic_interval.start) &
                   (regions.c.start <= genomic_interval.stop) &
                   (regions.c.chrom == str(genomic_interval.chrom)))

        if self.log_sql_commands:
            utils.show_stmt(connection, stmt, self.logger.debug, f'TCGA: VARIANTS IN REGION '
                                                                 f'{genomic_interval.chrom}'
                                                                 f'-{genomic_interval.start}-{genomic_interval.stop}')
        return stmt

    def take_regions_of_common_individuals(self, tables: list):
        """
        Generates a table containing all the mutations from all the origin tables but only for those individuals that
        appear in all the origin tables.
        Supposing that each origin table reflects a characteristic that the final sample set must have, this method
        basically puts those characteristics in AND relationship by taking only the regions from the individuals that
        have all the characteristics.
        :param tables: The source tables which must have the same columns in the same order.
        """
        if len(tables) == 1:
            return tables[0]
        else:
            # join 1st with 2nd with 3rd ... with nth on item_id
            # TODO consider creating temporary tables selecting only the item_id before joining
            stmt_join = reduce(
                lambda table_1, table_2: table_1.join(table_2, tables[0].c.item_id == table_2.c.item_id),
                tables)
            # union of tables
            select_all_from_each_table = map(lambda table_: select([table_]), tables)
            # TODO consider selecting from union table only what is needed by the users of this method (parametric choice)
            stmt_union = union(*select_all_from_each_table).alias()
            # select from the union table only the item_id that exists in the join
            stmt_as = \
                select([stmt_union]) \
                .where(exists(select()
                              .select_from(stmt_join)
                              .where(stmt_union.c.item_id == tables[0].c.item_id)
                              ))
            target_t_name = utils.random_t_name_w_prefix('intersect')
            stmt_create_table = utils.stmt_create_table_as(target_t_name, stmt_as, default_schema_to_use_name)
            if self.log_sql_commands:
                utils.show_stmt(self.connection, stmt_create_table, self.logger.debug,
                                'SELECT ALL FROM SOURCE TABLES WHERE item_id IS IN ALL SOURCE TABLES')
            self.connection.execute(stmt_create_table)
            # TODO drop partial tables ?
            return Table(target_t_name, db_meta, autoload=True, autoload_with=self.connection,
                         schema=default_schema_to_use_name)

    def get_variant_details(self, connection: Connection, variant: Mutation, which_details: List[Vocabulary],
                            assembly) -> list:
        self.connection = connection
        global regions
        select_columns = []
        for att in which_details:
            mapping = self.region_col_map.get(att)
            if mapping is not None:
                select_columns.append(regions.c[mapping].label(att.name))
            else:
                select_columns.append(cast(literal(Vocabulary.unknown.name), types.String).label(att.name))

        stmt = select(select_columns).distinct()
        if variant.chrom is not None:
            stmt = stmt.where((regions.c.chrom == str(variant.chrom)) &
                              (regions.c.start == variant.start) &
                              (regions.c.ref == variant.ref) &
                              (regions.c.alt == variant.alt))
        else:
            stmt = stmt.where(regions.c.id == variant.id)
        stmt = stmt.where(regions.c.item_id.in_(
                        select([metadata.c.item_id])
                        .where(metadata.c.assembly == assembly)
                    ))
        if self.log_sql_commands:
            utils.show_stmt(connection, stmt, self.logger.debug, 'GET VARIANT DETAILS')
        result = connection.execute(stmt)
        if result.rowcount == 0:
            return list()
        else:
            if result.rowcount > 1:
                self.logger.error(f'user searched for variant: chrom {str(variant.chrom)}, start {str(variant)}, '
                                  f'ref {str(variant.ref)}, alt {str(variant.alt)}, id {str(variant.id)}'
                                  f'but two results were found')
            final_result = result.fetchone().values()
            result.close()
            return final_result
