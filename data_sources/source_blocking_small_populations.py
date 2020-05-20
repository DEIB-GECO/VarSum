from typing import Tuple
from sqlalchemy import func, column
from data_sources.source_interface import *
from database.database import *


# noinspection PyAbstractClass
class SourceBlockingSmallPopulations(Source):
    """
    Specialization of class Source implementing a simple privacy policy to protect the identity of the project
    participants. Subclasses must call the constructor of this class passing as first argument the minimum acceptable
    population size, followed by all the arguments required by class Source.

    Subclasses must implement all the methods of class Source, but prefixing the method donors, variants_in_region,
    rank_variants_by_frequency and variants_in_region with an underscore. Everything else, including the arguments received
    are exactly as in Source.
    """

    def __init__(self, population_lower_threshold: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.POPULATION_LOWER_THRESHOLD = population_lower_threshold

    # SUBCLASSES MUST IMPLEMENT
    def _donors(self, *args, **kwargs):
        """Refer to the documentation of class Source"""
        raise NotImplementedError()

    def _variant_occurrence(self, *args, **kwargs):
        """Refer to the documentation of class Source"""
        raise NotImplementedError

    def _rank_variants_by_frequency(self, *args, **kwargs):
        """Refer to the documentation of class Source"""
        raise NotImplementedError()

    def _variants_in_region(self, *args, **kwargs) -> Tuple[FromClause, FromClause]:
        """
        The source must return a table or a statement selecting the variants located inside the given "genomic_interval".
        Each variant must be provided with the "output_region_attrs" requested.
        :param connection:
        :param genomic_interval:
        :param output_region_attrs:
        :param meta_attrs:
        :param region_attrs:
        :return: the statement or table containing the regions, and the statement or table returning the donors in the
        selected population
        """
        raise NotImplementedError()

    # PLUS THE OTHER METHODS FROM SOURCE

    # PRIVACY POLICY
    def block_if_below_threshold(self, table_or_stmt: FromClause):
        new_stmt = select([func.count()]).select_from(table_or_stmt.alias())
        population_size = try_stmt(new_stmt, None, None).scalar()
        print('SELECTED POPULATION_SIZE ', population_size)
        if population_size < self.POPULATION_LOWER_THRESHOLD:
            raise Notice(f'{self.__class__.__name__}: The selected query does not comply with the privacy constraints imposed by the '
                         f'source and the result was removed from the output. Please relax some filters to reach a '
                         f'population of at least {self.POPULATION_LOWER_THRESHOLD} individuals.')

    def donors(self, *args, **kwargs):
        table_or_stmt: FromClause = self._donors(*args, **kwargs)
        self.block_if_below_threshold(table_or_stmt)
        return table_or_stmt

    def variants_in_region(self, *args, **kwargs):
        variants_in_region, donors = self._variants_in_region(*args, **kwargs)
        self.block_if_below_threshold(donors)
        return variants_in_region

    def variant_occurrence(self, *args, **kwargs):
        table_or_stmt = self._variant_occurrence(*args, **kwargs)
        self.block_if_below_threshold(table_or_stmt)
        return table_or_stmt

    def rank_variants_by_frequency(self, *args, **kwargs):
        table_or_stmt = self._rank_variants_by_frequency(*args, **kwargs)
        count_not_allowed_populations_query = \
            select([func.count(column(Vocabulary.POPULATION_SIZE.name))])\
            .select_from(table_or_stmt.alias())\
            .where(column(Vocabulary.POPULATION_SIZE.name) < self.POPULATION_LOWER_THRESHOLD)
        count_not_allowed_populations = try_stmt(count_not_allowed_populations_query, None, None).scalar()
        if count_not_allowed_populations > 0:
            self.notify_message(SourceMessage.Type.GENERAL_WARNING,
                                f'{self.__class__.__name__}: The selected query does not comply with the privacy constraints imposed by the '
                                f'source and the result was removed from the output. Please relax some filters to reach a '
                                f'population of at least {self.POPULATION_LOWER_THRESHOLD} individuals.')
            return \
                select([table_or_stmt.alias()])\
                .where(column(Vocabulary.POPULATION_SIZE.name) >= self.POPULATION_LOWER_THRESHOLD)
        else:
            return table_or_stmt

