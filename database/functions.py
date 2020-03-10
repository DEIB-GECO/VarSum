from sqlalchemy import *
from sqlalchemy.engine import ResultProxy
from database import create_view_module, create_table_module
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

    log_sql_commands: bool = False
    default_metadata_table_name = 'genomes_metadata'
    default_metadata_schema_name = 'dw'
    default_region_table_name = 'genomes_full_data_red'  # 100 samples
    # default_region_table_name = 'genomes_full_data_red'  # 2535 samples
    default_region_schema_name = 'rr'

    def __init__(self, connection):
        self.connection = connection
        self.db_meta = MetaData()
        # reflect already existing tables (u can access columns as <table>.c.<col_name> or <table>.c['<col_name>'])
        # TODO try moving these two tables out of this class: u load them with engine pass them as constructor parameters
        self.metadata = Table(self.default_metadata_table_name, self.db_meta, autoload=True, autoload_with=self.connection,
                              schema=self.default_metadata_schema_name)
        self.genomes = Table(self.default_region_table_name, self.db_meta, autoload=True, autoload_with=self.connection,
                             schema=self.default_region_schema_name)

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
        result: engine.ResultProxy = self.connection.execute(_query)
        return result

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

    def _stmt_all_regions_with_mutation(self, mutation: Mutation):
        """
        :param mutation: Type Mutation obj
        :return: the SQL statement querying for all the regions from all samples that match the given mutation
        """
        query = select([self.genomes])
        if mutation.id is not None:
            query = query.where(self.genomes.c.id == mutation.id)
        else:  # don't need further checks: the existence of (chrom, start, alt) is already checked in the constructor of Mutation
            query = query.where(self.genomes.c.chrom == mutation.chrom) \
                .where(self.genomes.c.start == mutation.start) \
                .where(self.genomes.c.alt == mutation.alt)
        if self.log_sql_commands:
            self.show_stmt(query, 'QUERY ALL THE REGIONS CORRESPONDING TO A SINGLE MUTATION')
        return query

    @staticmethod
    def _stmt_where_region_is_any_of_mutations(*mutations: Mutation, from_table: str, select_expression):
        """
        :param mutations:
        :param from_table:
        :param select_expression:
        :return: the SQL statement querying all the regions from the given source region table, where the regions
        matches one of the given mutations. The returned query selects only the properties given in select_expression.
        """
        mutations_having_id = [mut for mut in mutations if mut.id is not None]
        mutations_without_id = [mut for mut in mutations if mut.id is None]
        first_select, second_select = None, None
        if len(mutations_having_id) > 0:
            first_select = select_expression.where(from_table.c.id.in_([mut.id for mut in mutations_having_id]))
        if len(mutations_without_id) > 0:
            second_select = select_expression.where(tuple_(from_table.c.start, from_table.c.alt, from_table.c.chrom).in_(
                [(mut.start, mut.alt, mut.chrom) for mut in mutations_without_id]
            ))
        if first_select is not None and second_select is not None:
            return union_all(first_select, second_select)
        elif first_select is not None:
            return first_select
        else:
            return second_select

    # GENERATE DB ENTITIES

    def view_of_samples_with_metadata(self, generated_view_name: str, gender: Optional[str], health_status: Optional[str],
                                      dna_source: Optional[list],
                                      assembly: str, population: Optional[list], super_population: Optional[list]):
        if population is not None:
            super_population = None
        arguments = locals()
        free_dimensions = {key for key in arguments.keys() if arguments[key] is None}
        print('free dimensions', free_dimensions)
        constrained_dimensions = set(arguments.keys()).difference(free_dimensions).discard('self')
        print('constrained dimensions', constrained_dimensions)
        columns_in_select = [self.metadata.c.item_id] + [self.metadata.c[col] for col in free_dimensions]
        query = select(columns_in_select)
        if gender:
            query = query.where(self.metadata.c.gender == gender)
        if health_status:
            query = query.where(self.metadata.c.health_status == health_status)
        if dna_source:
            query = query.where(self.metadata.c.dna_source.in_(dna_source))
        if assembly:
            query = query.where(self.metadata.c.assembly == assembly)
        if population:
            query = query.where(self.metadata.c.population.in_(population))
        elif super_population:
            query = query.where(self.metadata.c.super_population.in_(super_population))
        view = self.stmt_create_view_as(generated_view_name, query, 'dw')
        if self.log_sql_commands:
            self.show_stmt(view, 'VIEW OF SAMPLES HAVING META')
        self.connection.execute(view)

    def _table_with_any_of_mutations(self, generated_region_table_name: str, into_schema: str, *mutations: Mutation):
        """
        Computes the set of regions having mutation 1, or mutation 2, or mutation 3,... including duplicate item_id and puts
        it into table generated_region_table_name.
        :param generated_region_table_name: the name of the table that will contain the result set
        :param mutations: list of Mutation
        """
        if len(mutations) == 0:
            raise ValueError('function argument *mutations cannot be empty')
        elif len(mutations) == 1:
            stmt_as = self._stmt_all_regions_with_mutation(mutations[0])
        else:
            stmt_as = self._stmt_where_region_is_any_of_mutations(*mutations,
                                                                  from_table=self.genomes,
                                                                  select_expression=select([self.genomes]))

        # create table for the result
        stmt_create_table = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
        if self.log_sql_commands:
            self.show_stmt(stmt_create_table, 'CREATE TABLE HAVING ANY OF THE {} MUTATIONS'.format(len(mutations)))
        self.connection.execute(stmt_create_table)

    def table_with_all_of_mutations(self, generated_region_table_name: str, into_schema: str, *mutations: Mutation):
        """
        Generates a table containing the given mutations, and only from the individuals that own all the given mutations.
        :param into_schema: schema where to generate the result table
        :param generated_region_table_name: a name, unique for each call, which is the name of the
        :param mutations: a list of Mutations
        """
        if len(mutations) == 0:
            raise ValueError('function argument *mutations cannot be empty')
        elif len(mutations) == 1:
            stmt_create_table = self.stmt_create_table_as(generated_region_table_name,
                                                          self._stmt_all_regions_with_mutation(mutations[0]),
                                                          into_schema)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table, 'INDIVIDUALS HAVING THE MUTATION')
            self.connection.execute(stmt_create_table)
        else:
            union_table_name = 'intermediate_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
            self._table_with_any_of_mutations(union_table_name, into_schema, *mutations)
            union_table = Table(union_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=into_schema)
            # extracts only the samples having all the mutations
            stmt_as = \
                select([union_table])\
                .where(union_table.c.item_id.in_(
                    select([union_table.c.item_id])
                    .group_by(union_table.c.item_id)
                    .having(func.count(union_table.c.item_id) == len(mutations))
                    ))
            stmt_create_table = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table,
                               'INDIVIDUALS HAVING "ALL" THE {} MUTATIONS (WITH DUPLICATE ITEM_ID)'.format(len(mutations)))
            self.connection.execute(stmt_create_table)
            if self.log_sql_commands:
                print('DROP TABLE ' + union_table_name)
            union_table.drop(self.connection)

    def table_mutations_on_different_chrom_copies(self, generated_region_table_name: str, into_schema: str, mutation1: Mutation, mutation2: Mutation):
        """
        Generates a table containing the given mutations, and only from the individuals that own all the given mutations
        on opposite chromosome copies.
        :param generated_region_table_name: name of table to generate, containing the result set.
        :param into_schema: schema where to generate the output table.
        :param mutation1: a Mutation instance.
        :param mutation2: a Mutation instance.
        """
        regions_having_all_mutations_table = self.genomes
        # selects only the mutations to be on the different chromosome copies (this set will be used two times) from all individuals
        # we will enforce the presence of the mutations in all the individuals later...
        intermediate_table_name = 'intermediate_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
        self._table_with_any_of_mutations(intermediate_table_name, into_schema, mutation1, mutation2)
        intermediate_table = Table(intermediate_table_name, self.db_meta, autoload=True, autoload_with=self.connection,
                                   schema=into_schema)

        # groups mutations by owner in the intermediate table, and take only the owners for which sum(al1) = 1 and sum(al2) = 1
        # that condition automatically implies the presence of both mutations for the same owner
        # for those owner, take both mutations
        stmt_as = select([intermediate_table]) \
            .where(intermediate_table.c.item_id.in_(
                select([intermediate_table.c.item_id])
                .group_by(intermediate_table.c.item_id)
                .having((func.sum(intermediate_table.c.al1) == 1) &  # the ( ) around each condition are mandatory
                        (func.sum(func.coalesce(intermediate_table.c.al2, 0)) == 1))
        ))
        stmt = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'INDIVIDUALS (+ THE GIVEN MUTATIONS) HAVING BOTH MUTATIONS ON OPPOSITE CHROMOSOME COPIES')
        self.connection.execute(stmt)
        if self.log_sql_commands:
            print('DROP TABLE '+intermediate_table_name)
        intermediate_table.drop(self.connection)

    def table_mutations_on_same_chrom_copy(self, generated_region_table_name: str, into_schema: str, *mutations):
        """
        Generates a table containing the given mutations, and only from the individuals that own all the given mutations
        on the same chromosome copy.
        :param generated_region_table_name: name of table to generate, containing the result set.
        :param into_schema: schema where to generate the output table.
        :param mutations: a list of Mutation instances.
        """
        if len(mutations) < 2:
            raise ValueError('You must provide at least two Mutation instances in order to use this method.')
        regions_having_all_mutations_table = self.genomes
        # selects only the mutations to be on the same chromosome copies (this set will be used two times) from all individuals
        # we will enforce the presence of all the given mutations in all the individuals later...
        intermediate_table_name = 'intermediate_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
        self._table_with_any_of_mutations(intermediate_table_name, into_schema, *mutations)
        intermediate_table = Table(intermediate_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=into_schema)

        # groups mutations by owner in the intermediate table, and take only the owners for which sum(al1) or sum(al2)
        # equals to the number of the given mutations. That condition automatically implies the presence of all the
        # given mutations in the same individual.
        # for those owner, take all the given mutations
        stmt_as = select([intermediate_table]) \
            .where(intermediate_table.c.item_id.in_(
                select([intermediate_table.c.item_id])
                .group_by(intermediate_table.c.item_id)
                .having((func.sum(intermediate_table.c.al1) == len(mutations)) |  # the ( ) around each condition are mandatory
                        (func.sum(func.coalesce(intermediate_table.c.al2, 0)) == len(mutations)))
        ))
        stmt = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'INDIVIDUALS (+ THE GIVEN MUTATIONS) HAVING ALL THE SPECIFIED MUTATIONS ON THE SAME CHROMOSOME COPY')
        self.connection.execute(stmt)
        if self.log_sql_commands:
            print('DROP TABLE '+intermediate_table_name)
        intermediate_table.drop(self.connection)

    # takes individuals (+ their mutations) appearing in all source tables
    def take_regions_of_common_individuals(self, generated_table_name: str, into_schema: str, table_names: list, from_schema: list):
        """
        Generates a table containing all the mutations from all the origin tables but only for those individuals that
        appear in all the origin tables.
        Supposing that each origin table reflects a characteristic that the final sample set must have, this method
        basically puts those characteristics in AND relationship by taking only the regions from the individuals that
        have all the characteristics.
        :param generated_table_name:
        :param into_schema:
        :param table_names: The names of the source tables, which must have the same columns in the same order.
        :param from_schema: The schemas of the source tables, in the position of the tables they refer to.
        """
        if len(table_names) == 1:
            return table_names[0]
        else:
            # reflect tables
            origin_tables = [Table(name, self.db_meta, autoload=True, autoload_with=self.connection, schema=schema_)
                             for name, schema_ in zip(table_names, from_schema)]
            # join 1st with 2nd with 3rd ... with nth on item_id
            stmt_join = reduce(
                lambda table_1, table_2: table_1.join(table_2, origin_tables[0].c.item_id == table_2.c.item_id),
                origin_tables)
            # union of tables
            select_all_from_each_table = map(lambda table_: select([table_]), origin_tables)
            stmt_union = union(*select_all_from_each_table).alias()
            # select from the union table only the item_id that exists in the join
            stmt_as = select([stmt_union]).where(exists(select()
                                                        .select_from(stmt_join)
                                                        .where(stmt_union.c.item_id == origin_tables[0].c.item_id)
                                                        )
                                                 )
            stmt_create_table = self.stmt_create_table_as(generated_table_name, stmt_as, into_schema)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table, 'SELECT ALL FROM SOURCE TABLES WHERE item_id IS IN ALL SOURCE TABLES')
            self.connection.execute(stmt_create_table)

    # STATISTICS

    def count_samples_by_dimensions(self, samples_view_name: str, region_table_name: Optional[str], from_schema: str) -> ResultProxy:
        """
        :param samples_view_name: the name of the view describing the chosen samples
        :param region_table_name: name of the region
        :param from_schema: schema where region_table_name is located
        :return: The count of the samples grouped by each free dimension with cube
        """
        sample_view = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.connection, schema='dw') if samples_view_name is not None else self.metadata
        free_dimension_columns = [sample_view.c[col.name] for col in sample_view.columns if col.name != 'item_id']
        stmt = select([func.count().label('samples')] + free_dimension_columns)
        if region_table_name is not None:
            region_table = Table(region_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=from_schema)
            stmt = stmt.where(sample_view.c.item_id.in_(
                    select([region_table.c.item_id]).distinct()
            ))
        stmt = stmt.group_by(func.cube(*free_dimension_columns))
        if self.log_sql_commands:
            self.show_stmt(stmt, 'QUERY COUNT SAMPLES WITH CUBE ON FREE DIMENSIONS')
        return self.connection.execute(stmt)

    # works only if region_table_name is not None ('cos it must group by mutation and by free dimensions... too expensive on all mutations)
    def mutation_frequency_by_dimensions(self, samples_view_name: Optional[str], region_table_name: str, from_schema: str) -> ResultProxy:
        """
        Computes the distribution of each mutation with respect to the free dimensions, telling
        the number of samples that own it and the number of occurrences.
        :param samples_view_name: the name of the view containing the samples of interest.
        :param region_table_name: the name of the table containing the regions of of interest
        :param from_schema: the schema owning the source region table.
        :return: the result set.
        """
        if region_table_name is None:
            raise ValueError('You must provide a subset of mutations with respect to produce the statistics. '
                             'Avoid to give the whole genomes table, because this function groups by mutation')
        region_table = Table(region_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=from_schema)
        sample_view = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.connection, schema='dw') if samples_view_name is not None else self.metadata
        free_dimension_columns = [sample_view.c[col.name] for col in sample_view.columns if col.name != 'item_id']

        # defines custom functions
        func_occurrence = (func.sum(region_table.c.al1) + func.sum(func.coalesce(region_table.c.al2, 0))).label('occurrence')
        func_samples = func.count(region_table.c.item_id).label('samples')
        func_frequency = func.rr.mut_frequency(func_occurrence, func_samples, region_table.c.chrom).label('frequency')

        stmt = select([
            # here I select chrom, start, alt and then I group by these, however it is possible to do so also by id
            # but id may be null.
                      region_table.c.chrom, region_table.c.start, region_table.c.alt,
                      func_occurrence, func_samples, func_frequency
                      ] + free_dimension_columns) \
            .select_from(region_table.join(sample_view, sample_view.c.item_id == region_table.c.item_id)) \
            .group_by(region_table.c.chrom, region_table.c.start, region_table.c.alt, func.cube(*free_dimension_columns))\
            .order_by(region_table.c.chrom, region_table.c.start, region_table.c.alt)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'QUERY MUT FREQUENCY WITH CUBE ON FREE DIMENSIONS')
        return self.connection.execute(stmt)

    def most_common_mut_in_sample_set(self, samples_view_name: str, region_table_name: Optional[str], from_schema: Optional[str]):
        if samples_view_name is None:
            raise ValueError('sample view name cannot be None. Please use some filter condition.')
        sample_view = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.connection,
                            schema='dw')

        # consider only variants owned by the individuals in sample_view
        from_table = self.genomes.join(right=sample_view, onclause=self.genomes.c.item_id == sample_view.c.item_id)
        if region_table_name is not None:
            # consider only variants owned by the individuals in sample view and in region_table
            region_table = Table(region_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=from_schema)
            from_table = from_table.join(right=region_table, onclause=region_table.c.item_id == sample_view.c.item_id)

        # defines custom functions
        func_occurrence = (func.sum(self.genomes.c.al1) + func.sum(func.coalesce(self.genomes.c.al2, 0))).label('occurrence')
        func_samples = func.count(self.genomes.c.item_id).label('samples')
        func_frequency = func.rr.mut_frequency(func_occurrence, func_samples, self.genomes.c.chrom).label('frequency')

        stmt = select([self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt, func_occurrence, func_samples, func_frequency])\
            .select_from(from_table)\
            .group_by(self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt) \
            .order_by(desc(func_frequency)).limit(5)

        if self.log_sql_commands:
            self.show_stmt(stmt, 'MOST FREQUENT MUTATIONS IN SAMPLE SET')
        return self.connection.execute(stmt)

    def rarest_mut_in_sample_set(self, samples_view_name: str, region_table_name: Optional[str], from_schema: Optional[str]):
        if samples_view_name is None:
            raise ValueError('sample view name cannot be None. Please use some filter condition.')
        sample_view = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.connection,
                            schema='dw')

        # consider only variants owned by the individuals in sample_view
        from_table = self.genomes.join(right=sample_view, onclause=self.genomes.c.item_id == sample_view.c.item_id)
        if region_table_name is not None:
            # consider only variants owned by the individuals in sample view and in region_table
            region_table = Table(region_table_name, self.db_meta, autoload=True, autoload_with=self.connection, schema=from_schema)
            from_table = from_table.join(right=region_table, onclause=region_table.c.item_id == sample_view.c.item_id)

        # defines custom functions
        func_occurrence = (func.sum(self.genomes.c.al1) + func.sum(func.coalesce(self.genomes.c.al2, 0))).label('occurrence')
        func_samples = func.count(self.genomes.c.item_id).label('samples')
        func_frequency = func.rr.mut_frequency(func_occurrence, func_samples).label('frequency')

        stmt = select([self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt, func_occurrence, func_samples, func_frequency])\
            .select_from(from_table)\
            .group_by(self.genomes.c.chrom, self.genomes.c.start, self.genomes.c.alt)\
            .order_by(asc(func_frequency)).limit(5)

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

