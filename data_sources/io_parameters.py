from typing import Optional
from enum import Enum


class Mutation:

    def __init__(self, chrom: int = None, start: int = None, ref: str = None, alt: str = None, _id: str = None):
        """
        :param int chrom:
        :param int start:
        :param str ref:
        :param str alt:
        :param str _id:
        :return:
        """
        if (chrom is None or start is None or ref is None or alt is None) and _id is None:
            raise VariantUndefined(
                "Cannot identify Mutation. One between ID and (chrom, start, ref, alt) must be provided. "
                f"Input was: ID {_id} CHROM {chrom} START {start} REF {ref} ALT {alt}")
        self.id = _id
        self.chrom = chrom
        self.start = start
        self.ref = ref
        self.alt = alt

    @staticmethod
    def from_dict(mutation_dict: dict):
        if mutation_dict.get('id') is not None:
            return Mutation(_id=mutation_dict['id'])
        elif mutation_dict.get('chrom') is not None \
                and mutation_dict.get('start') is not None \
                and mutation_dict.get('ref') is not None \
                and mutation_dict.get('alt') is not None:
            return Mutation(chrom=mutation_dict['chrom'], start=mutation_dict['start'], ref=mutation_dict['ref'], alt=mutation_dict['alt'])
        else:
            raise VariantUndefined("Cannot identify Mutation. One between ID and (chrom, start, ref, alt) must be provided. "
                                   f"Input was: ID {mutation_dict.get('id')} CHROM {mutation_dict.get('chrom')} "
                                   f" START {mutation_dict.get('start')} REF {mutation_dict.get('ref')} "
                                   f"ALT {mutation_dict.get('alt')}")


class VariantUndefined(Exception):
    pass


class RegionAttrs:

    def __init__(self,
                 with_variants: Optional[list] = None,
                 with_variants_same_c_copy: Optional[list] = None,
                 with_variants_diff_c_copy: Optional[list] = None,
                 something_else: Optional[list] = None
                 ):
        self.requires = set()

        if with_variants:
            self.with_variants = with_variants
            self.requires.add(Vocabulary.WITH_VARIANT)
        else:
            self.with_variants = None

        if with_variants_same_c_copy:
            self.with_variants_same_c_copy = with_variants_same_c_copy
            self.requires.add(Vocabulary.WITH_VARIANT_SAME_C_COPY)
        else:
            self.with_variants_same_c_copy = None

        if with_variants_diff_c_copy:
            self.with_variants_diff_c_copy = with_variants_diff_c_copy
            self.requires.add(Vocabulary.WITH_VARIANT_DIFF_C_COPY)
        else:
            self.with_variants_diff_c_copy = None

        # TODO REMOVE
        if something_else:
            self.something_else = something_else
            self.requires.add(Vocabulary.WITH_SOMETHING_ELSE)
        else:
            self.something_else = None

        self.with_variants_in_reg: Optional[dict] = None
        self.with_variants_of_type: Optional[list] = None


class MetadataAttrs:

    def __init__(self,
                 gender: Optional[str] = None,
                 health_status: Optional[str] = None,
                 dna_source: Optional[list] = None,
                 assembly: str = None,
                 population: Optional[list] = None,
                 super_population: Optional[list] = None,
                 something_else: Optional['str'] = None):
        self.free_dimensions = []
        self.constrained_dimensions = []

        # TODO REMOVE
        if something_else:
            self.something_else = something_else
            self.constrained_dimensions.append(Vocabulary.SOMETHING_ELSE)
        else:
            self.something_else = None
            self.free_dimensions.append(Vocabulary.SOMETHING_ELSE)

        if gender:
            self.gender = gender
            self.constrained_dimensions.append(Vocabulary.GENDER)
        else:
            self.gender = None
            self.free_dimensions.append(Vocabulary.GENDER)

        if health_status:
            self.health_status = health_status
            self.constrained_dimensions.append(Vocabulary.GENDER.HEALTH_STATUS)
        else:
            self.health_status = None
            self.free_dimensions.append(Vocabulary.HEALTH_STATUS)

        if dna_source:
            self.dna_source = dna_source
            self.constrained_dimensions.append(Vocabulary.DNA_SOURCE)
        else:
            self.dna_source = None
            self.free_dimensions.append(Vocabulary.DNA_SOURCE)

        if assembly:
            self.assembly = assembly
            self.constrained_dimensions.append(Vocabulary.ASSEMBLY)
        else:
            self.assembly = None
            self.free_dimensions.append(Vocabulary.ASSEMBLY)

        if population:
            self.population = population
            self.constrained_dimensions.append(Vocabulary.POPULATION)
        else:
            self.population = None
            self.free_dimensions.append(Vocabulary.POPULATION)

        if super_population:
            self.super_population = super_population
            self.constrained_dimensions.append(Vocabulary.SUPER_POPULATION)
        else:
            self.super_population = None
            self.free_dimensions.append(Vocabulary.SUPER_POPULATION)


class Vocabulary(Enum):
    # dimensions of metadata kind
    GENDER = 1
    HEALTH_STATUS = 2
    DNA_SOURCE = 3
    ASSEMBLY = 4
    POPULATION = 5
    SUPER_POPULATION = 6
    SOMETHING_ELSE = 7  # TODO remove
    DONOR_ID = 8

    # dimensions of region kind
    WITH_VARIANT = 101
    WITH_VARIANT_SAME_C_COPY = 102
    WITH_VARIANT_DIFF_C_COPY = 103
    WITH_SOMETHING_ELSE = 104   # TODO remove
    # TODO extend with region intervals and type

    # measures
    FREQUENCY = 201
    COUNT = 202
    OCCURRENCE = 203
    POPULATION_SIZE = 204
    POSITIVE_DONORS = 205

    # identifiers of a variation
    CHROM = 401
    START = 402
    REF = 403
    ALT = 404

    # special values
    unknown = 301
