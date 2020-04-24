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
                "Cannot identify the Variant. One between ID and (chrom, start, ref, alt) must be provided. "
                f"Input was: ID {_id} CHROM {chrom} START {start} REF {ref} ALT {alt}")
        self.id = _id
        self.chrom = chrom
        self.start = start
        self.ref = ref
        self.alt = alt

    def __str__(self):
        return f'Variant ID {self.id} COORDINATES {self.chrom}-{self.start}-{self.ref}-{self.alt}'


class VariantUndefined(Exception):
    pass


class GenomicInterval:
    def __init__(self, chrom: int, start: int, stop: int, strand: Optional[int] = None):
        if strand is not None and not (strand == 1 or strand == -1):
            raise GenomicIntervalUndefined('When present, the strand of a genomic interval can have only values 1 and -1.')
        if chrom is None or start is None or stop is None:
            raise GenomicIntervalUndefined('A genomic interval needs at least the attributes chrom, start, stop '
                                           f'to have non-null values. Instead {str(chrom)}, {str(start)}, {str(stop)} '
                                           'were given.')
        if start > stop:
            raise GenomicIntervalUndefined('Your genomic interval has start coordinate > stop coordinate.')
        self.chrom = chrom
        self.start = start
        self.stop = stop
        self.strand = strand

    def __str__(self):
        return f'GenomicInterval {self.chrom}:{self.start}-{self.stop} {self.strand}'


class GenomicIntervalUndefined(Exception):
    pass


class Gene:
    def __init__(self, name: str, type_: Optional[str] = None, id_: Optional[str] = None):
        self.name = name
        if type_:
            self.type_ = type_
        else:
            self.type_ = None
        if id_:
            self.id_ = id_
        else:
            self.id_ = None


class RegionAttrs:

    def __init__(self,
                 with_variants: Optional[list] = None,
                 with_variants_same_c_copy: Optional[list] = None,
                 with_variants_diff_c_copy: Optional[list] = None,
                 with_variants_in_genomic_region: Optional[GenomicInterval] = None,
                 with_variants_in_gene: Optional[Gene] = None,
                 in_cell_type: Optional[list] = None
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

        if with_variants_in_genomic_region:
            self.with_variants_in_reg = with_variants_in_genomic_region
            self.requires.add(Vocabulary.WITH_VARIANT_IN_GENOMIC_INTERVAL)
        else:
            self.with_variants_in_reg = None

        if with_variants_in_gene:
            self.with_variants_in_gene = with_variants_in_gene
            self.requires.add(Vocabulary.WITH_VARIANT_IN_GENOMIC_INTERVAL)
        else:
            self.with_variants_in_gene = None

        if in_cell_type:
            if 'germline' in in_cell_type:
                self.requires.add(Vocabulary.WITH_VARIANTS_IN_GERMLINE_CELLS)
            if 'somatic' in in_cell_type:
                self.requires.add(Vocabulary.WITH_VARIANTS_IN_SOMATIC_CELLS)

        self.with_variants_of_type: Optional[list] = None


class MetadataAttrs:

    def __init__(self,
                 gender: Optional[str] = None,
                 health_status: Optional[str] = None,
                 dna_source: Optional[list] = None,
                 assembly: str = None,
                 population: Optional[list] = None,
                 super_population: Optional[list] = None,
                 ethnicity: Optional[list] = None):
        self.free_dimensions = []
        self.constrained_dimensions = []

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
            self.assembly = assembly.lower()
            self.constrained_dimensions.append(Vocabulary.ASSEMBLY)
        else:
            self.assembly = None
            self.free_dimensions.append(Vocabulary.ASSEMBLY)

        if population:
            self.population = population
            self.constrained_dimensions.append(Vocabulary.POPULATION)
            # consistency rule: super_population and ethnicity are free
            self.super_population, self.ethnicity = None, None
            self.free_dimensions.extend([Vocabulary.SUPER_POPULATION, Vocabulary.ETHNICITY])
        elif super_population:
            self.super_population = super_population
            self.constrained_dimensions.append(Vocabulary.SUPER_POPULATION)
            # consistency rule: population and ethnicity are free
            self.population, self.ethnicity = None, None
            self.free_dimensions.extend([Vocabulary.POPULATION, Vocabulary.ETHNICITY])
        elif ethnicity:
            self.ethnicity = ethnicity
            self.constrained_dimensions.append(Vocabulary.ETHNICITY)
            # consistency rule: population and super_population are free
            self.population, self.super_population = None, None
            self.free_dimensions.extend([Vocabulary.POPULATION, Vocabulary.SUPER_POPULATION])
        else:
            self.population, self.super_population, self.ethnicity = None, None, None
            self.free_dimensions.extend([Vocabulary.POPULATION, Vocabulary.SUPER_POPULATION, Vocabulary.ETHNICITY])


class Vocabulary(Enum):
    # dimensions of metadata kind
    GENDER = 1
    HEALTH_STATUS = 2
    DNA_SOURCE = 3
    ASSEMBLY = 4
    POPULATION = 5
    SUPER_POPULATION = 6
    DOWNLOAD_URL = 7
    DONOR_ID = 8
    ETHNICITY = 9

    # dimensions of region kind
    WITH_VARIANT = 101
    WITH_VARIANT_SAME_C_COPY = 102
    WITH_VARIANT_DIFF_C_COPY = 103
    WITH_VARIANT_IN_GENOMIC_INTERVAL = 104
    WITH_VARIANTS_IN_GERMLINE_CELLS = 105
    WITH_VARIANTS_IN_SOMATIC_CELLS = 106
    # TODO extend with region intervals and type

    # measures
    FREQUENCY = 201
    COUNT = 202
    OCCURRENCE = 203
    POPULATION_SIZE = 204
    POSITIVE_DONORS = 205

    # identifiers of a region
    CHROM = 401
    START = 402
    STOP = 403
    STRAND = 404
    LENGTH = 405

    # identifiers specific to variation data
    REF = 501
    ALT = 502
    ID = 503
    VAR_TYPE = 504
    QUALITY = 505
    FILTER = 506

    # identifiers specific to genes
    GENE_NAME = 601
    GENE_TYPE = 602
    GENE_ID = 603

    # special values
    unknown = 301


class Notice(Exception):
    """
    Container for a message signaling a problem. The field msg is directed to the user and it's shown to to him as a
    notice indicating why a given source couldn't provide an answer. This object and subclasses are reserved for
    "normal" or expected conditions which make the source not able to provide an answer. This do not replace
    other Exceptions though! If you encounter in a real error, then you should raise an Exception.
    Notice is the only expected way for a source to communicate to the coordinator returning something that isn't the
    answer.
    An example use case for Notice is the impossibility to return the answer due to privacy constraints.
    """
    def __init__(self, msg_explaining_cause_of_error: str):
        self.msg = msg_explaining_cause_of_error


class EmptyResult(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class SourceMessage:
    """
    This class can be used by any source willing to communicate a problem affecting the result directly to the user. If
    a source uses this warning, the source is expected to provide still a valid result, but the warning message will be
    attached to the final response together with the normal body of the response. In order to send this message, the
    source must have received a callback function from the Coordinator accepting objects of this class.
    """

    class Type(Enum):
        TIME_TO_FINISH = 1
        GENERAL_WARNING = 2

    def __init__(self, msg_type: Type, msg: str):
        self.type = msg_type
        self.msg = msg
