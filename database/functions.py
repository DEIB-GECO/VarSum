from collections import abc

from sqlalchemy import MetaData, Table, text, engine, select, union_all, union, tuple_, func, exists, asc, desc, intersect
from sqlalchemy.engine import ResultProxy
from database import create_view_module, create_table_module
from database.db_entities import *
from mutation_adt import Mutation
from prettytable import PrettyTable
from datetime import datetime
from functools import reduce
from typing import Optional

POPULATIONS_GROUPS = {
    'EUR': ['GBR', 'FIN', 'IBS', 'TSI', 'CEU'],
    'AFR': ['ASW', 'ECB', 'ESN', 'GWD', 'LWK', 'MSL', 'YRI'],
    'AMR': ['CLM', 'MXL', 'PEL', 'PUR'],
    'EAS': ['CDX', 'CHB', 'JPT', 'KHV', 'CHS'],
    'SAS': ['BEB', 'GIH', 'ITU', 'PJL', 'STU']
}


# noinspection PyPropertyAccess
class DBFunctions:
    """
    This class is slightly improved thanks to the reduction of parameters asked in the select statements.
    Before:
        we always did select* from genomes
    Now:
        select item_id, chrom, start, alt, al1, al2, id
    """

    log_sql_commands: bool = False
    default_metadata_table_name = 'genomes_metadata'
    default_metadata_schema_name = 'dw'
    default_region_table_name = 'genomes_full_data_red'  # 100 samples
    # default_region_table_name = 'genomes_reduced_colset'  # 2535 samples
    default_region_schema_name = 'rr'
    default_schema_to_use_name = 'dw'

    def __init__(self, connection):
        self.connection = connection
        self.db_meta = MetaData()
        # reflect already existing tables (u can access columns as <table>.c.<col_name> or <table>.c['<col_name>'])
        # TODO try moving these two tables out of this class: u load them with engine pass them as constructor parameters
        self.metadata = Table(self.default_metadata_table_name, self.db_meta, autoload=True, autoload_with=self.connection,
                              schema=self.default_metadata_schema_name)
        self.genomes = Table(self.default_region_table_name, self.db_meta, autoload=True, autoload_with=self.connection,
                             schema=self.default_region_schema_name)
        self.meta_attrs: Optional[MetadataAttrs] = None
        self.region_attrs: Optional[RegionAttrs] = None
        self.my_meta_t: Optional[Table] = None
        self.my_region_t: Optional[Table] = None

    # SETTERS

    def _set_region_attributes(self, region_attrs: RegionAttrs):
        self.region_attrs = region_attrs
        self.my_region_t = None  # reset my region table

    def _set_meta_attributes(self, meta_attrs: MetadataAttrs):
        self.meta_attrs = meta_attrs
        self.my_meta_t = None  # reset my meta table

    # SQL BASIC COMMANDS

    def disconnect(self):
        self.connection.close()

    def exec_raw_query(self, query_string):
        """
        :param query_string: raw SQL query without the trailing ';'
        :return: a sqlalchemy.engine.ResultProxy object
        """
        _query = text(query_string + ';')
        print('###      EXECUTE RAW QUERY       ###')
        if self.log_sql_commands:
            self.show_stmt(_query)
        return self.connection.execute(_query)

    # VISUALIZE RESULTS AND QUERIES

    @staticmethod
    def print_query_result(result: ResultProxy):
        pretty_table = PrettyTable(result.keys())
        row = result.fetchone()
        while row:
            pretty_table.add_row(row)
            row = result.fetchone()
        print(pretty_table)

    def show_stmt(self, stmt, intro=None):
        # compiled_stmt = stmt.compile(compile_kwargs={"literal_binds": True}, dialect=postgresql.dialect())
        # #substitued by instr below
        if intro is not None:
            print('###   ' + intro + '   ###')
        compiled_stmt = stmt.compile(compile_kwargs={"literal_binds": True}, dialect=self.connection.dialect)
        print(str(compiled_stmt))

    def print_table_named(self, table_name: str, table_schema: str):
        if not table_name.islower():
            print('Postgre saves table names as lower case strings. I\'ll try to access your table as all lowercase.')
        table_to_print = Table(table_name.lower(), self.db_meta, autoload=True, autoload_with=self.connection, schema=table_schema)
        query_all = select([table_to_print])
        result = self.connection.execute(query_all)
        self.print_query_result(result)

    def print_table(self, table: Table):
        query_all = select([table])
        result = self.connection.execute(query_all)
        self.print_query_result(result)

    # SQL STATEMENT GENERATORS

    @staticmethod
    def random_t_name_w_prefix(prefix: str):
        return prefix + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')

    @staticmethod
    def stmt_create_view_as(name: str, select_stmt, into_schema):
        return create_view_module.CreateView('"' + into_schema + '".' + name, select_stmt)

    def drop_view(self, name: str, from_schema: str):
        self.exec_raw_query('DROP VIEW "'+from_schema+'".'+name)

    @staticmethod
    def stmt_create_table_as(name: str, select_stmt, into_schema):
        return create_table_module.CreateTableAs('"' + into_schema + '".' + name, select_stmt)

    def create_table_as(self, name: str, select_stmt, into_schema):
        compiled_select = select_stmt.compile(compile_kwargs={"literal_binds": True}, dialect=self.connection.dialect)
        stmt = 'CREATE TABLE "' + into_schema + '".' + name + ' AS '+str(compiled_select)
        self.exec_raw_query(stmt)

    # def _stmt_all_regions_with_mutation(self, mutation: Mutation):
    #     """
    #     :param mutation: Type Mutation obj
    #     :return: the SQL statement querying for all the regions from all samples that match the given mutation
    #     """
    #     query = select([self.genomes])
    #     if mutation.id is not None:
    #         query = query.where(self.genomes.c.id == mutation.id)
    #     else:  # don't need further checks: the existence of (chrom, start, alt) is already checked in the constructor of Mutation
    #         query = query.where(self.genomes.c.chrom == mutation.chrom) \
    #             .where(self.genomes.c.start == mutation.start) \
    #             .where(self.genomes.c.alt == mutation.alt)
    #     if self.log_sql_commands:
    #         self.show_stmt(query, 'QUERY ALL THE REGIONS CORRESPONDING TO A SINGLE MUTATION')
    #     return query

    # @staticmethod
    # def columns_for_table(col_names: abc, of_table: Table, if_col_names_is_none):
    #     if col_names is None:
    #         return if_col_names_is_none
    #     else:
    #         return [of_table.c[name] for name in col_names]

    @staticmethod
    def _stmt_where_region_is_any_of_mutations(*mutations: Mutation, from_table, select_expression, only_item_id_in_table: Optional[Table]):
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
            second_select = select_expression.where(tuple_(from_table.c.start, from_table.c.alt, from_table.c.chrom).in_(
                [(mut.start, mut.alt, mut.chrom) for mut in mutations_without_id]
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
        if self.meta_attrs.population is not None:
            self.meta_attrs.super_population = None
        columns_in_select = [self.metadata]  # take all columns by default
        if select_columns is not None:       # otherwise take the ones in select_columns but make sure item_id is present
            temp_set = set(select_columns)
            temp_set.add('item_id')
            columns_in_select = [self.metadata.c[col_name] for col_name in temp_set]
        query = select(columns_in_select)
        if self.meta_attrs.gender:
            query = query.where(self.metadata.c.gender == self.meta_attrs.gender)
        if self.meta_attrs.health_status:
            query = query.where(self.metadata.c.health_status == self.meta_attrs.health_status)
        if self.meta_attrs.dna_source:
            query = query.where(self.metadata.c.dna_source.in_(self.meta_attrs.dna_source))
        if self.meta_attrs.assembly:
            query = query.where(self.metadata.c.assembly == self.meta_attrs.assembly)
        if self.meta_attrs.population:
            query = query.where(self.metadata.c.population.in_(self.meta_attrs.population))
        elif self.meta_attrs.super_population:
            query = query.where(self.metadata.c.super_population.in_(self.meta_attrs.super_population))
        new_meta_table_name = self.random_t_name_w_prefix('meta')
        self.create_table_as(new_meta_table_name, query, self.default_schema_to_use_name)
        # t_stmt = self.stmt_create_table_as(new_meta_table_name, query,  self.default_schema_to_use_name)
        # if self.log_sql_commands:
        #     self.show_stmt(t_stmt, 'TABLE OF SAMPLES HAVING META')
        # self.connection.execute(t_stmt)
        self.my_meta_t = Table(new_meta_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=self.default_schema_to_use_name)

    def create_table_of_regions(self, select_columns: Optional[list]):
        # compute each filter on regions separately
        to_combine_t = list()
        if self.region_attrs.with_variants:
            t = self.table_with_all_of_mutations(select_columns)
            to_combine_t.append(t)
        if self.region_attrs.with_variants_same_c_copy:
            t = self.table_with_variants_same_c_copy(select_columns)
            to_combine_t.append(t)
        if self.region_attrs.with_variants_diff_c_copy:
            t = self.table_with_variants_on_diff_c_copies(select_columns)
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
            if select_columns is not None:    # otherwise use select_columns + minimum necessary
                union_select_column_names = set(select_columns)
                union_select_column_names.add('item_id')
            union_table = self._table_with_any_of_mutations(union_select_column_names, self.my_meta_t, *self.region_attrs.with_variants)
            # extracts only the samples having all the mutations
            result_select_columns = [union_table]  # means all columns
            if select_columns is not None:         # otherwise use selected_columns
                result_select_columns = [union_table.c[col_name] for col_name in select_columns]
            stmt_as = \
                select(result_select_columns)\
                .where(union_table.c.item_id.in_(
                    select([union_table.c.item_id])
                    .group_by(union_table.c.item_id)
                    .having(func.count(union_table.c.item_id) == len(self.region_attrs.with_variants))
                ))
            target_t_name = self.random_t_name_w_prefix('with')
            stmt_create_table = self.stmt_create_table_as(target_t_name, stmt_as, self.default_schema_to_use_name)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table,
                               'INDIVIDUALS HAVING "ALL" THE {} MUTATIONS (WITH DUPLICATE ITEM_ID)'.format(
                                   len(self.region_attrs.with_variants)))
            self.connection.execute(stmt_create_table)
            if self.log_sql_commands:
                print('DROP TABLE ' + union_table.name)
            union_table.drop(self.connection)
            return Table(target_t_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=self.default_schema_to_use_name)

    def _table_with_any_of_mutations(self, select_columns, only_item_id_in_table: Optional[Table], *mutations: Mutation):
        """Returns a Table containing all the rows from the table self.genomes containing one of the variants in
        the argument mutations.
        :param select_columns selects only the column names in this collection. If None, selects all the columns from self.genomes.
        :param only_item_id_in_table If None, the variants that are not owned by any of the individuals in this table
        are discarded from the result.
        """
        if len(mutations) == 0:
            raise ValueError('function argument *mutations cannot be empty')
        else:
            # create table for the result
            t_name = self.random_t_name_w_prefix('with_any_of_mut')
            columns = [self.genomes.c[c_name] for c_name in select_columns] if select_columns is not None else [self.genomes]
            stmt_as = self._stmt_where_region_is_any_of_mutations(*mutations,
                                                                  from_table=self.genomes,
                                                                  select_expression=select(columns),
                                                                  only_item_id_in_table=only_item_id_in_table)
        stmt_create_table = self.stmt_create_table_as(t_name, stmt_as, self.default_schema_to_use_name)
        if self.log_sql_commands:
            self.show_stmt(stmt_create_table, 'CREATE TABLE HAVING ANY OF THE {} MUTATIONS'.format(len(mutations)))
        self.connection.execute(stmt_create_table)
        return Table(t_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=self.default_schema_to_use_name)

    def table_with_variants_same_c_copy(self, select_columns: Optional[list]):
        """
         Returns a table of variants of the same type of the ones contained in RegionAttrs.with_variants_same_c_copy and only
         form the individuals that own all of them on the same chromosome copy.
         :param select_columns: the list of column names to select from the result. If None, all the columns are taken.
        """
        if len(self.region_attrs.with_variants_same_c_copy) < 2:
            raise ValueError('You must provide at least two Mutation instances in order to use this method.')
        # selects only the mutations to be on the same chromosome copies (this set will be used two times) from all individuals
        # we will enforce the presence of all the given mutations in all the individuals later...
        interm_select_column_names = None   # means all columns
        if select_columns is not None:      # otherwise pick select_columns + minimum required
            interm_select_column_names = set(select_columns)
            interm_select_column_names.update(['item_id', 'al1', 'al2'])
        intermediate_table = self._table_with_any_of_mutations(interm_select_column_names, self.my_meta_t, *self.region_attrs.with_variants_same_c_copy)
        # groups mutations by owner in the intermediate table, and take only the owners for which sum(al1) or sum(al2)
        # equals to the number of the given mutations. That condition automatically implies the presence of all the
        # given mutations in the same individual.
        # for those owner, take all the given mutations
        result_columns = [intermediate_table]   # means all columns
        if select_columns is not None:          # otherwise pick the columns from select_columns
            result_columns = [intermediate_table.c[col_name] for col_name in select_columns]
        stmt_as = \
            select(result_columns) \
            .where(intermediate_table.c.item_id.in_(
                select([intermediate_table.c.item_id])
                .group_by(intermediate_table.c.item_id)
                .having(
                    (func.sum(intermediate_table.c.al1) == len(self.region_attrs.with_variants_same_c_copy)) |  # the ( ) around each condition are mandatory
                    (func.sum(func.coalesce(intermediate_table.c.al2, 0)) == len(self.region_attrs.with_variants_same_c_copy)))
            ))
        target_t_name = self.random_t_name_w_prefix('with_var_same_c_copy')
        stmt = self.stmt_create_table_as(target_t_name, stmt_as, self.default_schema_to_use_name)
        if self.log_sql_commands:
            self.show_stmt(stmt,
                           'INDIVIDUALS (+ THE GIVEN MUTATIONS) HAVING ALL THE SPECIFIED MUTATIONS ON THE SAME CHROMOSOME COPY')
        self.connection.execute(stmt)
        if self.log_sql_commands:
            print('DROP TABLE ' + intermediate_table.name)
        intermediate_table.drop(self.connection)
        return Table(target_t_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=self.default_schema_to_use_name)

    def table_with_variants_on_diff_c_copies(self, select_columns: Optional[list]):
        """
         Returns a table of variants of the same type of the ones contained in RegionAttrs.with_variants_diff_c_copy and only
         form the individuals that own both of them on opposite chromosome copies.
         :param select_columns: the list of column names to select from the result. If None, all the columns are taken.
        """
        if len(self.region_attrs.with_variants_diff_c_copy) != 2:
            raise ValueError('You must provide exactly two Mutation instances in order to use this method.')
        # selects only the mutations to be on the different chromosome copies (this set will be used two times) from all individuals
        # we will enforce the presence of the mutations in all the individuals later...
        interm_select_column_names = None  # means all columns
        if select_columns is not None:  # otherwise pick select_columns + minimum required
            interm_select_column_names = set(select_columns)
            interm_select_column_names.update(['item_id', 'al1', 'al2'])
        intermediate_table = self._table_with_any_of_mutations(interm_select_column_names, self.my_meta_t, *self.region_attrs.with_variants_diff_c_copy)
        # groups mutations by owner in the intermediate table, and take only the owners for which sum(al1) = 1 and sum(al2) = 1
        # that condition automatically implies the presence of both mutations for the same owner
        # for those owner, take both mutations
        result_columns = [intermediate_table]  # means all columns
        if select_columns is not None:  # otherwise pick the columns from select_columns
            result_columns = [intermediate_table.c[col_name] for col_name in select_columns]
        stmt_as = \
            select(result_columns) \
            .where(intermediate_table.c.item_id.in_(
                select([intermediate_table.c.item_id])
                .group_by(intermediate_table.c.item_id)
                .having(
                    (func.sum(intermediate_table.c.al1) == 1) &  # the ( ) around each condition are mandatory
                    (func.sum(func.coalesce(intermediate_table.c.al2, 0)) == 1))
            ))
        target_t_name = self.random_t_name_w_prefix('with_var_diff_c_copies')
        stmt = self.stmt_create_table_as(target_t_name, stmt_as, self.default_schema_to_use_name)
        if self.log_sql_commands:
            self.show_stmt(stmt,
                           'INDIVIDUALS (+ THE GIVEN MUTATIONS) HAVING BOTH MUTATIONS ON OPPOSITE CHROMOSOME COPIES')
        self.connection.execute(stmt)
        if self.log_sql_commands:
            print('DROP TABLE ' + intermediate_table.name)
        intermediate_table.drop(self.connection)
        return Table(target_t_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=self.default_schema_to_use_name)

    def view_of_variants_in_interval_or_type(self, select_columns: Optional[list]):
        if self.region_attrs.with_variants_in_reg is None and self.region_attrs.with_variants_of_type is None:
            raise ValueError('you called this method without giving any selection criteria')
        columns = [self.genomes.c[c_name] for c_name in select_columns] if select_columns is not None else [self.genomes]
        stmt_as = select(columns)
        if self.region_attrs.with_variants_in_reg is not None:
            chrom = self.region_attrs.with_variants_in_reg['chrom']
            left_end = self.region_attrs.with_variants_in_reg['left']
            right_end = self.region_attrs.with_variants_in_reg['right']
            if chrom is None or left_end is None or right_end is None:
                raise ValueError('the given genomic interval is not complete')
            stmt_as = stmt_as.where((self.genomes.c.chrom == chrom) &
                                    (self.genomes.c.start >= left_end) &
                                    (self.genomes.c.stop <= right_end))
        if self.region_attrs.with_variants_of_type is not None:
            stmt_as = stmt_as.where(self.genomes.c.mut_type.in_(self.region_attrs.with_variants_of_type))
        generated_view_name = self.random_t_name_w_prefix('mut_of_type_interval')
        stmt = self.stmt_create_view_as(generated_view_name, stmt_as, self.default_schema_to_use_name)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'VIEW OF REGIONS IN INTERVAL {} of types {}'.format(self.region_attrs.with_variants_in_reg, self.region_attrs.with_variants_of_type))
        self.connection.execute(stmt)

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
                select([stmt_union])\
                .where(exists(select()
                              .select_from(stmt_join)
                              .where(stmt_union.c.item_id == tables[0].c.item_id)
                              ))
            target_t_name = self.random_t_name_w_prefix('intersect')
            stmt_create_table = self.stmt_create_table_as(target_t_name, stmt_as, self.default_schema_to_use_name)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table, 'SELECT ALL FROM SOURCE TABLES WHERE item_id IS IN ALL SOURCE TABLES')
            self.connection.execute(stmt_create_table)
            # TODO drop partial tables ?
            return Table(target_t_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=self.default_schema_to_use_name)

    # STATISTICS

    def count_samples_by_dimensions(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs) -> ResultProxy:
        """
        :return: The count of the samples grouped by each free dimension with cube
        """
        # init state
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(self.meta_attrs.free_dimensions)
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])

        # compute statistics
        free_dimension_columns = [self.my_meta_t.c[col_name] for col_name in meta_attrs.free_dimensions]
        stmt = select([func.count().label('samples')] + free_dimension_columns)
        if self.my_region_t is not None:
            stmt = stmt.where(self.my_meta_t.c.item_id.in_(
                    select([self.my_region_t.c.item_id]).distinct()
            ))
        stmt = stmt.group_by(func.cube(*free_dimension_columns))
        if self.log_sql_commands:
            self.show_stmt(stmt, 'QUERY COUNT SAMPLES WITH CUBE ON FREE DIMENSIONS')
        return self.connection.execute(stmt)

    def distribution_of_variant(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, variant: Mutation, by_attributes: list):
        """
        Computes the distribution of each mutation with respect to the free dimensions, telling
        the number of samples that own it and the number of occurrences.
        :return: the result set.
        """
        # init state
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(['item_id'])
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])
        if self.my_region_t is None:
            raise ValueError('Before using this method, you need to assign a valid state to the region attributes at least.'
                             'Avoid to give the whole genomes table, because this function groups by mutation')
        if not by_attributes:
            raise ValueError('You must specify at lest one attribute in order to compute the distribution.')
        # compute sample set. Actually, self.my_region_t already contains only the individuals comaptible with meta_attrs,
        # however we can ease the future operations by removing duplicates.
        sample_set = intersect(self.my_meta_t.select(), self.my_region_t.select()).alias()

        # defines custom functions
        func_occurrence = (func.sum(self.genomes.c.al1) + func.sum(func.coalesce(self.genomes.c.al2, 0))).label('occurrence')
        func_samples = func.count(self.genomes.c.item_id).label('samples')
        func_frequency = func.rr.mut_frequency(func_occurrence, func_samples, self._get_chrom_of_variant(variant)).label('frequency')
        #TODO fix frequency: occurrence / numerosity of sample set * ploidy
        columns_in_select = [self.metadata.c[col_name] for col_name in by_attributes]
        
        stmt = select([func_occurrence, func_samples, func_frequency] + columns_in_select)
        stmt = self._stmt_where_region_is_any_of_mutations(variant,
                                                           from_table=self.genomes,
                                                           select_expression=stmt,
                                                           only_item_id_in_table=sample_set)
        stmt = stmt\
            .where(self.metadata.c.item_id == self.genomes.c.item_id)\
            .group_by(func.cube(*columns_in_select))
        if self.log_sql_commands:
            self.show_stmt(stmt, 'QUERY MUT FREQUENCY WITH CUBE ON FREE DIMENSIONS')
        return self.connection.execute(stmt)

    def most_common_mut_in_sample_set(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs):
        # init state
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(['item_id'])
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])
        if self.my_region_t is None:
            raise ValueError('Before using this method, you need to assign a valid state to the region attributes at least.'
                             'Please specify some region constraint.')
        # compute sample set. Actually, self.my_region_t already contains only the individuals comaptible with meta_attrs,
        # however we can ease the future operations by removing duplicates.
        sample_set = intersect(self.my_meta_t.select(), self.my_region_t.select()).alias()

        # defines custom functions
        func_occurrence = (func.sum(self.genomes.c.al1) + func.sum(func.coalesce(self.genomes.c.al2, 0))).label('occurrence')
        func_samples = func.count(self.genomes.c.item_id).label('samples')
        func_frequency = func.rr.mut_frequency(func_occurrence, func_samples, self.genomes.c.chrom).label('frequency')

        stmt = select([self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt, func_occurrence, func_samples, func_frequency])\
            .select_from(self.genomes.join(sample_set, self.genomes.c.item_id == sample_set.c.item_id))\
            .group_by(self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt) \
            .order_by(desc(func_frequency), desc(func_occurrence))\
            .limit(5)

        if self.log_sql_commands:
            self.show_stmt(stmt, 'MOST FREQUENT MUTATIONS IN SAMPLE SET')
        return self.connection.execute(stmt)

    def rarest_mut_in_sample_set(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs):
        # init state
        self._set_meta_attributes(meta_attrs)
        self.create_table_of_meta(['item_id'])
        self._set_region_attributes(region_attrs)
        self.create_table_of_regions(['item_id'])
        if self.my_region_t is None:
            raise ValueError(
                'Before using this method, you need to assign a valid state to the region attributes at least.'
                'Please specify some region constraint.')
        # compute sample set. Actually, self.my_region_t already contains only the individuals comaptible with meta_attrs,
        # however we can ease the future operations by removing duplicates.
        sample_set = intersect(self.my_meta_t.select(), self.my_region_t.select()).alias()

        # defines custom functions
        func_occurrence = (func.sum(self.genomes.c.al1) + func.sum(func.coalesce(self.genomes.c.al2, 0))).label('occurrence')
        func_samples = func.count(self.genomes.c.item_id).label('samples')
        func_frequency = func.rr.mut_frequency(func_occurrence, func_samples, self.genomes.c.chrom).label('frequency')

        stmt = select([self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt, func_occurrence, func_samples, func_frequency])\
            .select_from(self.genomes.join(sample_set, self.genomes.c.item_id == sample_set.c.item_id))\
            .group_by(self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt) \
            .order_by(asc(func_frequency), asc(func_occurrence))\
            .limit(5)

        if self.log_sql_commands:
            self.show_stmt(stmt, 'RAREST MUTATIONS IN SAMPLE SET')
        return self.connection.execute(stmt)

    # INFORMATIVE QUERIES

    def distinct_values_for(self, attribute_name):
        region_attributes = self.genomes.columns.keys()
        meta_attributes = self.metadata.columns.keys()
        print('regions:', region_attributes)
        print('meta:', meta_attributes)
        if attribute_name in meta_attributes:
            stmt = select([self.metadata.c[attribute_name]]).distinct()
        elif attribute_name in region_attributes:
            stmt = select([self.genomes.c[attribute_name]]).distinct()
        else:
            raise ValueError('the given attribute {} is not part of the metadata columns nor region columns'.format(attribute_name))
        if self.log_sql_commands:
            self.show_stmt(stmt, 'DISTINCT VALUES OF {}'.format(attribute_name))
        return self.connection.execute(stmt)

    def _get_chrom_of_variant(self, variant: Mutation):
        if variant.chrom is not None:
            return variant.chrom
        else:
            chrom_query = select([self.genomes.c.chrom]).where(self.genomes.c.id == variant.id).limit(1)
            return self.connection.execute(chrom_query).fetchone().values()[0]