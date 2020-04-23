from data_sources.io_parameters import *
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import FromClause
from typing import List, Callable


def do_not_notify(type: SourceMessage.Type, msg: str) -> None:
    return


class Source:
    """
        Base abstract class for all variation data sources. Any such source must declare the fields
        1. meta_col_map: a dictionary mapping io_parameters.Vocabulary names to the names in use inside the source,
        typically the names of the columns of the backing database table.
        2. avail_region_constraints: a set of io_parameters.Vocabulary expressing the kind of constraints that the source can
        enforce on region data in order to filter the variants.
        These fields support the coordinator and some methods of Source (those annotated with @classmethod). A
        subclass of Source can override them if necessary. Instead the other methods must be overridden.
        """

    meta_col_map: dict = {}
    avail_region_constraints: set = set()

    def __init__(self, logger_instance, notify_message: Callable[[SourceMessage.Type, str], None] = do_not_notify):
        self.logger = logger_instance
        self.notify_message = notify_message

    def donors(self, connection, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs,
               region_attrs: RegionAttrs, with_download_urls: bool) -> FromClause:
        """
        Requests a source to return the individuals having the characteristics in meta_attrs and region_attrs. For each
        return the attributes given in by_attributes (which always includes the donor identifier). Order is not important.
        Use the nomenclature available in Vocabulary for column names.
        :param connection: a sqlalchemy.engine.Connection
        :param by_attributes: a list of enums of the ones in Vocabulary and supported by this source (
        previously checked through get_available_attributes).
        :param meta_attrs: a MetadataAttrs.
        :param region_attrs: a RegionAttrs.
        :param with_download_urls: True if the source should add a column for the download url of the donors.
        :return: Returns the individuals identified by meta_attrs and region_attrs at the same time. The returned table
        or statement must have a column named as Vocabulary.DONOR_ID and the columns in by_attributes labeled with the
        names available in Vocabulary class.
        """
        raise NotImplementedError('Any subclass of Source must implement the abstract method "donors"')

    def variant_occurrence(self, connection: Connection, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs,
                           region_attrs: RegionAttrs, variant: Mutation) -> FromClause:
        """
        Requests a source to return the individuals having the characteristics in meta attrs and region attrs. For
        each of them specify
        - the occurrence of the target variant (as a number 0/1/2) in a column named as Vocabulary.OCCURRENCE.name.
        - the attributes given in by_attributes (which always includes the donor identifier).
        Order is not important. Use the nomenclature available in Vocabulary for column names.
        """
        raise NotImplementedError('Any subclass of Source must implement the abstract method "variant_occurrence".')

    def rank_variants_by_frequency(self, connection, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, ascending: bool,
                                   freq_threshold: float, limit_result: int) -> FromClause:
        """
        Requests a source to return the top "limit_result" variants in the most common/rarest variants in the selected
        population of individuals. The selected population is the one having meta_attrs and region_attrs.
        "freq_threshold" is a boundary; if the rarest variants are requested, it is the lowest possible frequency,
        otherwise it is the higher possible value of frequency.
        Each variant returned must be described by the columns describing the
        chromosome, start, reference, and alternative alleles. In addition to them, for each variant there must be also
        a value indicating the size of the population, the number of individuals owning the variant,
        the number of occurrences found such variant and finally the frequency. As always, refer to
        io_parameters.Vocabulary to find and use the appropriate names of the requested columns.
        :param connection:
        :param meta_attrs:
        :param region_attrs:
        :param ascending:
        :param freq_threshold:
        :param limit_result:
        :return:
        """
        raise NotImplementedError('Any subclass of Source must implement the abstract method "rank_variants_by_frequency".')

    def values_of_attribute(self, connection, attribute: Vocabulary) -> List:
        """
        Request a source to return a list of the values available for the "attribute".
        :param connection:
        :param attribute:
        :return:
        """
        raise NotImplementedError('Any subclass of Source must implement the abstract method "values_of_attribute".')

    def get_variant_details(self, connection, variant: Mutation, which_details: List[Vocabulary], assembly) -> List:
        """
        Given the "variant", the source must return a list of the values of the properties in "which_details" in the
        same order. If a property is not available, return Vocabulary.unknown.name in place of the unknown value.
        A common use case for this method is that for which is requested to know the region coordinates
        of a variant of which only the id is known.
        :param assembly:
        :param connection:
        :param variant:
        :param which_details:
        :return:
        """
        raise NotImplementedError('Any subclass of Source must implement the abstract method "get_variant_details".')

    def variants_in_region(self, connection: Connection, genomic_interval: GenomicInterval,
                           output_region_attrs: List[Vocabulary], assembly: str) -> FromClause:
        """
        The source must return a table or a statement selecting the variants located inside the given "genomic_interval".
        Each variant must be provided with the "output_region_attrs" requested.
        :param assembly:
        :param connection:
        :param genomic_interval:
        :param output_region_attrs:
        :return:
        """
        raise NotImplementedError('Any subclass of Source must implement the abstract method "variants_in_genomic_interval".')

    @classmethod
    def can_express_constraint(cls, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, method=None) -> bool:
        """
        This method is used to determine if a source can possibly select a population having the properties described
        in "meta_attrs" and "region_attrs". The default implementation requires a source to have a mapping between the
        active properties in "meta_attrs" and the table columns used internally by the source (field meta_col_map). As for
        the region constraints, the field "avail_region_constraints" in the source must have all the items in
        region_attrs".requires.
        :param meta_attrs:
        :param region_attrs:
        :param method: the method of class Coordinator invoking this method. Subclasses of Source can use this information
        to possibly customize the default behaviour.
        :return:
        """
        if len(cls.meta_col_map) == 0 or len(cls.avail_region_constraints) == 0:
            raise NotImplementedError('Source concrete implementations need to override class '
                                      'dictionary "meta_col_map" and set "avail_region_constraints"')
        has_all = True  # initial assumption
        if meta_attrs is not None:
            idx = 0
            while has_all and idx < len(meta_attrs.constrained_dimensions):
                has_all = cls.meta_col_map.get(meta_attrs.constrained_dimensions[idx]) is not None
                idx += 1
        if has_all and region_attrs is not None:
            has_all = cls.avail_region_constraints.issuperset(region_attrs.requires)
        return has_all

    @classmethod
    def get_available_attributes(cls):
        """
        Returns the Vocabulary items that the source supports in filtering, i.e. the ones mapped to sone table column
        name in "meta_col_map".
        :return:
        """
        if len(cls.meta_col_map) == 0:
            raise NotImplementedError('Source concrete implementations need to override class '
                                      'dictionary "meta_col_map" and set "avail_region_constraints"')
        return cls.meta_col_map.keys()
