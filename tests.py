from database.db_entities import *
from database.functions import DBFunctions
from sqlalchemy import create_engine
from mutation_adt import Mutation
import sys

db_user = sys.argv[1]
db_password = sys.argv[2]
db = None

if __name__ == '__main__':
    engine = create_engine('postgresql://{0}:{1}@localhost:15432/gmql_meta_new16_tommaso'.format(db_user, db_password))
    connection = engine.connect().execution_options(autocommit=True)
    db = DBFunctions(engine.connect())
    db.log_sql_commands = True

# these two are on different chromosome copies
mut1 = Mutation(_id='rs367896724')
mut2 = Mutation(_id='rs555500075')
mut2_fingerprint = Mutation(chrom=1, start=10351, alt='A')
# these two are on same chromosome copy
mut3_al1 = Mutation(_id='rs367896724')
mut3_fingerprint = Mutation(1, 10176, 'C')
mut4_al1 = Mutation(_id='rs376342519')
mut4_fingerprint = Mutation(1, 10615, '')

meta1 = MetadataAttrs(gender=None,
                      health_status='true',
                      dna_source=None,
                      assembly='hg19',
                      population=None,
                      super_population=['SAS'])

# FILTER BY METADATA
# db.meta_attrs = meta1
# db.create_table_of_meta()
# db.print_table(db.my_meta_t)


# HAVING MUTATIONS 3 AND 4
# db.meta_attrs = meta1
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
# db.meta_attrs = meta1
# db.region_attrs = RegionAttrs(
#     [mut4_al1],
#     None,
#     [mut1, mut2_fingerprint]
# )
# db.create_table_of_meta()   # this enables early filtering of individuals based on meta
# db.create_table_of_regions(['item_id'])
# db.print_table(db.my_region_t)


# MUTATION FREQUENCY BY DIMENSION
# db.meta_attrs = meta1
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


if db is not None:
    db.disconnect()
