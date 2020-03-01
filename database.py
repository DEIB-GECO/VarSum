from sqlalchemy import *
from sqlalchemy.engine import ResultProxy
import create_view_module
import create_table_module
from mutation import Mutation
from prettytable import PrettyTable
from datetime import datetime

POPULATIONS_GROUPS = {
    'EUR': ['GBR', 'FIN', 'IBS', 'TSI', 'CEU'],
    'AFR': ['ASW', 'ECB', 'ESN', 'GWD', 'LWK', 'MSL', 'YRI'],
    'AMR': ['CLM', 'MXL', 'PEL', 'PUR'],
    'EAS': ['CDX', 'CHB', 'JPT', 'KHV', 'CHS'],
    'SAS': ['BEB', 'GIH', 'ITU', 'PJL', 'STU']
}


class DBConnection:

    log_sql_commands: bool = False

    def __init__(self, user: str, password: str):
        # configuration
        self.engine = create_engine('postgresql://{0}:{1}@localhost:15432/gmql_meta_new16_tommaso'.format(user, password)) \
            .execution_options(autocommit=True)
        self.connection = self.engine.connect()
        self.db_meta = MetaData()
        # reflect already existing tables (u can access columns as <table>.c.<col_name> or <table>.c['<col_name>'])
        self.metadata = Table('genomes_metadata', self.db_meta, autoload=True, autoload_with=self.engine, schema='dw')
        self.genomes = Table('genomes_reduced_colset', self.db_meta, autoload=True, autoload_with=self.engine,
                             schema='rr')

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
        compiled_stmt = stmt.compile(compile_kwargs={"literal_binds": True}, dialect=self.engine.dialect)
        print(str(compiled_stmt))

    def print_table_named(self, table_name: str, table_schema: str):
        table_to_print = Table(table_name, self.db_meta, autoload=True, autoload_with=self.engine, schema=table_schema)
        query_all = select([table_to_print])
        result = self.connection.execute(query_all)
        self.print_query_result(result)

    # SQL STATEMENTS (NOTHING IS EXECUTED)

    @staticmethod
    def stmt_create_view_as(name: str, select_stmt, into_schema):
        return create_view_module.CreateView('"' + into_schema + '".' + name, select_stmt)

    @staticmethod
    def stmt_create_table_as(name: str, select_stmt, into_schema):
        return create_table_module.CreateTableAs('"' + into_schema + '".' + name, select_stmt)

    def _stmt_with_mutation(self, samples_view_name, mutation: Mutation):
        """
        :param samples_view_name: Type str. View or table containing the item_id of the samples respecting the metadata
        filters
        :param mutation: Type Mutation obj
        :return: the query
        """
        samples_view_repr = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.engine, schema='dw')
        # query the samples from samples_view_name having the region specified
        query = select([self.genomes]) \
            .select_from(samples_view_repr.join(self.genomes, samples_view_repr.c.item_id == self.genomes.c.item_id))
        if mutation.id is not None:
            query = query.where(self.genomes.c.id == mutation.id)
        else:  # don't need further checks: the existence of (chrom, start, alt) is already checked in the constructor of Mutation
            query = query.where(self.genomes.c.chom == mutation.chrom) \
                .where(self.genomes.c.start == mutation.start) \
                .where(self.genomes.c.alt == mutation.alt)
        if self.log_sql_commands:
            self.show_stmt(query, 'QUERY HAVING SINGLE MUTATION')
        return query

    @staticmethod
    def _stmt_add_where_mutation_is(*mutations: Mutation, from_table, select_expression):
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
            return union(first_select, second_select)
        elif first_select is not None:
            return first_select
        else:
            return second_select

    # SQL COMMANDS  (THEY WORK ONE ON TOP OF EACH OTHER)

    def view_of_samples_with_metadata(self, generated_view_name: str, gender: str = None, health_status: str = None,
                                      dna_source: list = None,
                                      assembly: str = None, population: list = None, super_population: list = None):
        if population is not None:
            super_population = None
        arguments = locals()
        free_dimensions = {key for key in arguments.keys() if arguments[key] is None}
        print('free dimensions', free_dimensions)
        constrained_dimensions = set(arguments.keys()).difference(free_dimensions)
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

    def _with_any_of_mutations(self, samples_view_name, generated_region_table_name, into_schema, *mutations: Mutation):
        """
        Computes the set of regions having mutation 1, or mutation 2, or mutation 3,... including duplicate item_id and puts
        it into table generated_region_table_name.
        :param samples_view_name: name of the view filtering the samples by metadata
        :param generated_region_table_name: the name of the table that will contain the result set
        :param mutations: list of Mutation
        """
        if len(mutations) == 0:
            raise ValueError('function argument *mutations cannot be empty')
        elif len(mutations) == 1:
            stmt_as = self._stmt_with_mutation(samples_view_name, mutations[0])
        else:
            print('###       PRINT INDIVIDUAL QUERIES FIRST      ###')
            single_queries = map(lambda mut: self._stmt_with_mutation(samples_view_name, mut), mutations)
            stmt_as = union_all(*single_queries)
            if self.log_sql_commands:
                self.show_stmt(stmt_as, 'QUERY HAVING_MUTATIONS ({} MUTATIONS)     ###'.format(len(mutations)))
        # create table for the result
        stmt_create_table = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
        if self.log_sql_commands:
            self.show_stmt(stmt_create_table)
        self.connection.execute(stmt_create_table)

    def with_all_mutations(self, samples_view_name, generated_region_table_name, into_schema, *mutations: Mutation):
        """
        Computes the regions of the samples from samples_view_name, having all the mutations specified as argument, and puts
        them into a table named generated_region_table_name.
        :param into_schema: schema where to generate the result table
        :param samples_view_name:
        :param generated_region_table_name: a name, unique for each call, which is the name of the
        :param mutations: a list of Mutations
        """
        if len(mutations) == 0:
            raise ValueError('function argument *mutations cannot be empty')
        elif len(mutations) == 1:
            stmt_as = self._stmt_with_mutation(samples_view_name, mutations[0])
            stmt_create_table = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table,
                               'REGIONS OF THE SAMPLES HAVING ALL MUTATIONS ({} MUTATIONS)'.format(len(mutations)))
            self.connection.execute(stmt_create_table)
        else:
            union_table_name = 'intermediate_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
            self._with_any_of_mutations(samples_view_name, union_table_name, into_schema, *mutations)
            union_table = Table(union_table_name, self.db_meta, autoload=True, autoload_with=self.engine, schema=into_schema)
            # extracts only the samples having all the mutations
            stmt_as = \
                select([union_table]) \
                    .where(union_table.c.item_id.in_(
                    select([union_table.c.item_id])
                        .group_by(union_table.c.item_id)
                        .having(func.count(union_table.c.item_id) == len(mutations))
                )
                )
            stmt_create_table = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
            if self.log_sql_commands:
                self.show_stmt(stmt_create_table,
                               'REGIONS OF THE SAMPLES HAVING ALL MUTATIONS ({} MUTATIONS)'.format(len(mutations)))
            self.connection.execute(stmt_create_table)
            union_table.drop(self.engine)

    def with_mutations_on_different_chrom_copies(self, regions_having_all_mutations_name, from_schema,
                                                 generated_region_table_name, into_schema, mutation1, mutation2):
        """
        Filters the regions by taking all the regions of the individuals respecting the following condition:
        if the individual owns mutation1 and mutation2, they must occur on opposite chromosome copies. For the individuals
         respecting this condition, all their regions from the source table are taken, not just the ones passed as
         argument to mutation1 or mutation2.
        :param regions_having_all_mutations_name: the source region table. Contains all the regions of the individuals
        selected before.
        :param from_schema: schema of regions_having_all_mutations_name.
        :param generated_region_table_name: name of table to generate, containing the result set.
        :param into_schema: schema where to generate the output table.
        :param mutation1: a Mutation instance.
        :param mutation2: a Mutation instance.
        :return: nothing, but creates a table named as generated_region_table_name holding the result.
        """
        regions_having_all_mutations_table = Table(regions_having_all_mutations_name, self.db_meta, autoload=True,
                                                   autoload_with=self.engine, schema=from_schema)

        # selects only the mutations to be on the different chromosome copies (this set will be used two times)
        intermediate_table_name = 'intermediate_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
        stmt_select_intermediate = self._stmt_add_where_mutation_is(mutation1, mutation2,
                                                                    from_table=regions_having_all_mutations_table,
                                                                    select_expression=select([regions_having_all_mutations_table]))
        stmt_intermediate_table = self.stmt_create_table_as(intermediate_table_name, stmt_select_intermediate, into_schema)

        if self.log_sql_commands:
            self.show_stmt(stmt_intermediate_table)
        self.connection.execute(stmt_intermediate_table)
        intermediate_table = Table(intermediate_table_name, self.db_meta, autoload=True, autoload_with=self.engine,
                                   schema=into_schema)

        # groups mutations by owner in the intermediate table, and take only the owners for which sum(al1) = 1 and sum(al2) = 1
        # for those owner, take all the mutations from the source region table
        stmt_as = select([regions_having_all_mutations_table]) \
            .where(regions_having_all_mutations_table.c.item_id.in_(
            select([intermediate_table.c.item_id])
                .group_by(intermediate_table.c.item_id)
                .having((func.sum(intermediate_table.c.al1) == 1) &  # the ( ) around each condition are mandatory
                        (func.sum(func.coalesce(intermediate_table.c.al2, 0)) == 1))
        ))
        stmt = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'REGIONS OF THE SAMPLES HAVING ALL MUTATIONS ON SAME CHROM COPY')
        self.connection.execute(stmt)
        if self.log_sql_commands:
            print('DROP TABLE '+intermediate_table_name)
        intermediate_table.drop(self.engine)

    def with_all_mutations_on_same_chrom_copy(self, regions_having_all_mutations_name, from_schema,
                                              generated_region_table_name, into_schema, *mutations):
        """
        Filters the regions by taking all the regions of the individuals respecting the following condition:
        if the individual owns the requested mutations, they must all occur on the same chromosome copies (either all
        on the first copy, or all on the second copy, or both). For the individuals respecting this condition, all their
        regions from the source table are taken, not just the ones passed as argument to mutations.
        :param regions_having_all_mutations_name: the source region table. Contains all the regions of the individuals
        selected before.
        :param from_schema: schema of regions_having_all_mutations_name.
        :param generated_region_table_name: name of table to generate, containing the result set.
        :param into_schema: schema where to generate the output table.
        :param mutations: a list of Mutation instances.
        :return: nothing, but creates a table named as generated_region_table_name holding the result.
        """
        if len(mutations) < 2:
            raise ValueError('You must provide at least two Mutation instances in order to use this method.')
        regions_having_all_mutations_table = Table(regions_having_all_mutations_name, self.db_meta, autoload=True,
                                                   autoload_with=self.engine, schema=from_schema)

        # selects only the mutations to be on the same chromosome copy (this set will be used two times)
        intermediate_table_name = 'intermediate_' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S_%f')
        stmt_select_intermediate = self._stmt_add_where_mutation_is(*mutations,
                                                                    from_table=regions_having_all_mutations_table,
                                                                    select_expression=select([regions_having_all_mutations_table]))
        stmt_intermediate_table = self.stmt_create_table_as(intermediate_table_name, stmt_select_intermediate, into_schema)

        if self.log_sql_commands:
            self.show_stmt(stmt_intermediate_table)
        self.connection.execute(stmt_intermediate_table)
        intermediate_table = Table(intermediate_table_name, self.db_meta, autoload=True, autoload_with=self.engine,
                                   schema=into_schema)

        stmt_as = select([regions_having_all_mutations_table]) \
            .where(regions_having_all_mutations_table.c.item_id.in_(
            select([intermediate_table.c.item_id])
                .group_by(intermediate_table.c.item_id)
                .having((func.sum(intermediate_table.c.al1) == len(mutations)) |  # the ( ) around each condition are mandatory
                        (func.sum(func.coalesce(intermediate_table.c.al2, 0)) == len(mutations)))
        ))
        stmt = self.stmt_create_table_as(generated_region_table_name, stmt_as, into_schema)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'REGIONS OF THE SAMPLES HAVING ALL MUTATIONS ON SAME CHROM COPY')
        self.connection.execute(stmt)
        intermediate_table.drop(self.engine)

    def count_samples_by_dimensions(self, samples_view_name):
        """
        :param samples_view_name: the name of the view describing the chosen samples
        :return: The count of the samples grouped by each free dimension with cube
        """
        sample_view = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.engine, schema='dw')
        free_dimension_columns = [sample_view.c[col.name] for col in sample_view.columns if col.name != 'item_id']
        stmt = select([func.count().label('samples')] + free_dimension_columns) \
            .group_by(func.cube(*free_dimension_columns))
        if self.log_sql_commands:
            self.show_stmt(stmt, 'QUERY COUNT SAMPLES WITH CUBE ON FREE DIMENSIONS')
        return self.connection.execute(stmt)

    def mutation_frequency_by_dimensions(self, samples_view_name, regions_of_samples_name, from_schema):
        """
        Computes the distribution of each mutation with respect to the free dimensions, telling
        the number of samples that own it and the number of occurrences.
        :param samples_view_name: the name of the view providing the samples.
        :param regions_of_samples_name: the name of the table containing the regions of the samples (after
            filtering on mutations as desired)
        :param from_schema: the schema owning the source region table.
        :return: the result set.
        """
        samples = Table(samples_view_name, self.db_meta, autoload=True, autoload_with=self.engine, schema='dw')
        free_dimension_columns = [samples.c[col.name] for col in samples.columns if col.name != 'item_id']
        regions = Table(regions_of_samples_name, self.db_meta, autoload=True, autoload_with=self.engine,
                        schema=from_schema)
        stmt = select([
            # here I select chrom, start, alt and then I group by these, however it is possible to do so also by id
            # but id may be null.
                          regions.c.chrom, regions.c.start, regions.c.alt,
                          func.count(regions.c.item_id).label('samples'),
                          (func.sum(regions.c.al1) + func.sum(func.coalesce(regions.c.al2, 0))).label('occurrence')
                      ] + free_dimension_columns) \
            .select_from(regions.join(samples, samples.c.item_id == regions.c.item_id)) \
            .group_by(regions.c.chrom, regions.c.start, regions.c.alt, func.cube(*free_dimension_columns))\
            .order_by(regions.c.chrom, regions.c.start, regions.c.alt)
        if self.log_sql_commands:
            self.show_stmt(stmt, 'QUERY MUT FREQUENCY WITH CUBE ON FREE DIMENSIONS')
        return self.connection.execute(stmt)


