from data_sources.io_parameters import *
import data_sources.coordinator as coordinator
from typing import Optional


if __name__ == '__main__':
    import database.database as database
    import sys
    db_user = sys.argv[1]
    db_password = sys.argv[2]
    database.config_db_engine_for_tests(db_user, db_password)

# these two are on different chromosome copies
mut1 = Mutation(_id='rs367896724')
mut2 = Mutation(_id='rs555500075')
mut2_fingerprint = Mutation(chrom=1, start=10351, alt='A')
# these two are on same chromosome copy
mut3_al1 = Mutation(_id='rs367896724')
mut3_fingerprint = Mutation(1, 10176, 'C')
mut4_al1 = Mutation(_id='rs376342519')
mut4_fingerprint = Mutation(1, 10615, '')

hg19_healthy_SAS = MetadataAttrs(gender=None,
                                 health_status='true',
                                 dna_source=None,
                                 assembly='hg19',
                                 population=None,
                                 super_population=['SAS'])
hg19_healthy_female_BEB = MetadataAttrs(gender='female',
                                        health_status='true',
                                        dna_source=None,
                                        assembly='hg19',
                                        population=['BEB'],
                                        super_population=None)
something_else = MetadataAttrs(something_else='bla')

# FILTER BY METADATA
# db.meta_attrs = hg19_healthy_SAS
# db.create_table_of_meta()
# db.print_table(db.my_meta_t)


# HAVING MUTATIONS 3 AND 4
# db.meta_attrs = hg19_healthy_SAS
# db.region_attrs = RegionAttrs(
#     [mut3_al1, mut4_al1],
#     None,
#     None
# )
# db.create_table_of_meta()   # this enables early filtering of individuals based on meta
# db.create_table_of_regions(None)
# db.print_table(db.my_region_t)


# HAVING MUTATIONS 3 AND 4 ON SAME CHROMOSOME COPY
# db.region_attrs = RegionAttrs(
#     None,
#     [mut3_al1, mut4_fingerprint],
#     None
# )
# db.create_table_of_regions(None)
# db.print_table(db.my_region_t)


# HAVING MUTATIONS 1 AND 2 ON DIFFERENT CHROMOSOME COPIES
# db.region_attrs = RegionAttrs(
#     None,
#     None,
#     [mut1, mut2_fingerprint]
# )
# db.create_table_of_regions(['item_id'])
# db.print_table(db.my_region_t)


# MAKES AND OF CHARACTERISTICS BY INDIVIDUAL
# db.meta_attrs = hg19_healthy_SAS
# db.region_attrs = RegionAttrs(
#     [mut4_al1],
#     None,
#     [mut1, mut2_fingerprint]
# )
# db.create_table_of_meta()   # this enables early filtering of individuals based on meta
# db.create_table_of_regions(['item_id'])
# db.print_table(db.my_region_t)


# MUTATION FREQUENCY BY DIMENSION
# db.meta_attrs = hg19_healthy_SAS
# db.region_attrs = RegionAttrs(
#     None,
#     None,
#     [mut1, mut2_fingerprint]
# )
# result = db.distribution_of_variant(MetadataAttrs(gender=None,
#                                                   health_status='true',
#                                                   dna_source=None,
#                                                   assembly='hg19',
#                                                   population=None,
#                                                   super_population=['AMR', 'SAS']),
#                                     RegionAttrs(
#                                         None,
#                                         None,
#                                         [mut1, mut2_fingerprint]
#                                      ),
#                                     mut4_al1,
#                                     ['gender', 'super_population'])
# db.print_query_result(result)


# COUNT OF SAMPLES BY DIMENSION
# result =    db.count_samples_by_dimensions(MetadataAttrs(gender=None,
#                                                          health_status='true',
#                                                          dna_source=None,
#                                                          assembly='hg19',
#                                                          population=None,
#                                                          super_population=['AMR', 'SAS']),
#                                            RegionAttrs(None,
#                                                        None,
#                                                        [mut1, mut2_fingerprint]))
# db.print_query_result(result)


# MOST COMMON MUTATIONS
# result = db.most_common_mut_in_sample_set(MetadataAttrs(gender='female',
#                                                         health_status='true',
#                                                         dna_source=None,
#                                                         assembly='hg19',
#                                                         population=['BEB'],
#                                                         super_population=None),
#                                           RegionAttrs(
#                                               [Mutation(1, 13271, 'C'), Mutation(1, 15272, 'T'), Mutation(1, 10176, 'C')],
#                                               None,
#                                               None
#                                            ))
# db.print_query_result(result)

# MUTATIONS IN REGION
# regions_in_interval_t_name = db.random_t_name_w_prefix('mut_in_intrv')
# db.view_of_mutations_in_interval_or_type(regions_in_interval_t_name, 'dw', 1, 0, 10176, None)
# db.view_of_mutations_in_interval_or_type(regions_in_interval_t_name, 'dw', None, None, None, mut_type=['SNP', 'CNV'])
# db.view_of_mutations_in_interval_or_type(regions_in_interval_t_name, 'dw', 1, 0, 10176, mut_type=['SNP', 'CNV', 'INS'])


# meta = io_param.MetadataAttrs(gender='a', health_status='a', dna_source=['a'], assembly='a', population=['a'], super_population=['a'])
# region = io_param.RegionAttrs(with_variants=['a'], with_variants_same_c_copy=['a'], with_variants_diff_c_copy=['a'])

by_attributes = [Vocabulary.POPULATION, Vocabulary.SOMETHING_ELSE]
# import data_sources.kgenomes.kgenomes as kgenomes
# import database.database as database
# import database.db_utils as db_utils
#
# source = kgenomes.KGenomes()
#
#
# def tr(connection):
#     stmt = source.most_common_mut_in_sample_set(connection, hg19_healthy_female_BEB, RegionAttrs([mut4_al1], None, [mut1, mut2_fingerprint]))
#     # result = connection.execute(stmt)
#     # db_utils.print_query_result(result)
#
#
# database.try_py_function(tr)

# result_proxy = coordinator.donor_distribution(by_attributes, hg19_healthy_female_BEB, None)
# result_proxy = coordinator.variant_distribution(by_attributes, hg19_healthy_female_BEB, RegionAttrs([mut1], None, None), mut2)
# result_proxy = coordinator.most_common_variants(hg19_healthy_female_BEB, RegionAttrs([mut1], None, None))
print(coordinator.values_of_attribute(Vocabulary.HEALTH_STATUS))
print(coordinator.values_of_attribute(Vocabulary.GENDER))
print(coordinator.values_of_attribute(Vocabulary.POPULATION))
print(coordinator.values_of_attribute(Vocabulary.SUPER_POPULATION))
print(coordinator.values_of_attribute(Vocabulary.DNA_SOURCE))