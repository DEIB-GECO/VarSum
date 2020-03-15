from typing import Optional


class RegionAttrs:
    # TODO extend with region intervals and type
    def __init__(self,
                 with_variants: Optional[list],
                 with_variants_on_same_c_copy: Optional[list],
                 with_variants_on_diff_c_copies: Optional[list],
                 ):
        self.with_variants = with_variants
        self.with_variants_same_c_copy = with_variants_on_same_c_copy
        self.with_variants_diff_c_copy = with_variants_on_diff_c_copies
        self.with_variants_in_reg: Optional[dict] = None
        self.with_variants_of_type: Optional[list] = None


class MetadataAttrs:
    
    def __init__(self,
                 gender: Optional[str],
                 health_status: Optional[str],
                 dna_source: Optional[list],
                 assembly: str,
                 population: Optional[list],
                 super_population: Optional[list]):
        
        self.free_dimensions = []
        self.gender = gender
        if self.gender is None:
            self.free_dimensions.append('gender')
        self.health_status = health_status
        if self.health_status is None:
            self.free_dimensions.append('health_status')
        self.dna_source = dna_source
        if self.dna_source is None:
            self.free_dimensions.append('dna_source')
        self.assembly = assembly
        if self.assembly is None:
            self.free_dimensions.append('assembly')
        self.population = population
        if self.population is None:
            self.free_dimensions.append('population')
        self.super_population = super_population
        if self.super_population is None:
            self.free_dimensions.append('super_population')
