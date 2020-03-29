from data_sources.io_parameters import *
from sqlalchemy.engine import Connection
from sqlalchemy.sql.expression import FromClause
from typing import List


class Source:
    """Base abstract class for all the sources."""

    meta_col_map: dict = {}
    avail_region_constraints: set = set()

    def donors(self, connection, by_attributes: List[Vocabulary], meta_attrs: MetadataAttrs,
               region_attrs: RegionAttrs) -> FromClause:
        """
        Requests a source to return the individuals having the characteristics in meta_attrs and region_attrs. For each
        return the attributes given in by_attributes (which always includes the donor identifier). Order is not important.
        Use the nomenclature available in Vocabulary for column names.
        :param connection: a sqlalchemy.engine.Connection
        :param by_attributes: a list of enums of the ones in Vocabulary and supported by this source (
        previously checked through get_available_attributes).
        :param meta_attrs: a MetadataAttrs.
        :param region_attrs: a RegionAttrs.
        :return: Returns the individuals identified by meta_attrs and region_attrs at the same time. The returned table must
        have a column named donor_id and the columns in by_attributes named as the enums in MetadataAttrs.Option.
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

    def most_common_variant(self, connection, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, out_max_freq: float, limit_result: int):
        raise NotImplementedError('Any subclass of Source must implement the abstract method "most_common_variant".')

    def rarest_variant(self, connection, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, out_min_freq: float, limit_result: int):
        raise NotImplementedError('Any subclass of Source must implement the abstract method "rarest_variant".')

    def values_of_attribute(self, connection, attribute: Vocabulary) -> List:
        raise NotImplementedError('Any subclass of Source must implement the abstract method "values_of_attribute".')

    def can_express_constraint(self, meta_attrs: MetadataAttrs, region_attrs: RegionAttrs, method=None):
        if len(self.meta_col_map) == 0 or len(self.avail_region_constraints) == 0:
            raise NotImplementedError('Source concrete implementations need to override class '
                                      'dictionary "meta_col_map" and set "avail_region_constraints"')
        has_all = True  # initial assumption
        if meta_attrs is not None:
            idx = 0
            while has_all and idx < len(meta_attrs.constrained_dimensions):
                has_all = self.meta_col_map.get(meta_attrs.constrained_dimensions[idx]) is not None
                idx += 1
        if has_all and region_attrs is not None:
            has_all = self.avail_region_constraints.issuperset(region_attrs.requires)
        return has_all

    def get_available_attributes(self):
        return self.meta_col_map.keys()
