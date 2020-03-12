from datetime import datetime
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

# FILTER BY METADATA
# view_of_samples_name = db.random_t_name_w_prefix('sample_view')
# db.view_of_samples_with_metadata(view_of_samples_name, health_status='true', assembly='hg19', super_population=['EAS'], gender=None,
#                                  dna_source=None, population=None)
# db.print_table_named(view_of_samples_name, 'dw')
# view_of_samples_name = 'sample_view_2020_02_19_16_58_51'
view_of_samples_name = 'sample_view_2020_03_05_03_35_23'  # super-pop EAS, assembly hg19, healthy

# these two are on different chromosome copies
mut1 = Mutation(_id='rs367896724')
mut2 = Mutation(_id='rs555500075')
mut2_fingerprint = Mutation(chrom=1, start=10351, alt='A')
# these two are on same chromosome copy
mut3_al1 = Mutation(_id='rs367896724')
mut3_fingerprint = Mutation(1, 10176, 'C')
mut4_al1 = Mutation(_id='rs376342519')
mut4_fingerprint = Mutation(1, 10615, '')


# HAVING MUTATIONS 3 AND 4
# samples_having_all_mutations_name = 'samples_with_mut3_and_4' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.table_with_all_of_mutations(samples_having_all_mutations_name, 'dw', mut3_al1, mut4_al1)
# db.print_table_named(samples_having_all_mutations_name, 'dw')
samples_having_all_mutations_name = 'insert name here'

# HAVING MUTATIONS 3 AND 4 ON SAME CHROMOSOME COPY
# samples_having_all_mutations_on_same_chrom_copy_name = db.random_t_name_w_prefix('with_mutations_on_same_chrom_copy')
# db.table_mutations_on_same_chrom_copy(samples_having_all_mutations_on_same_chrom_copy_name, 'dw', mut3_al1, mut4_al1)
# db.print_table_named(samples_having_all_mutations_on_same_chrom_copy_name, 'dw')
samples_having_all_mutations_on_same_chrom_copy_name = 'insert name here'


# HAVING MUTATIONS 1 AND 2 ON DIFFERENT CHROMOSOME COPIES
# samples_having_mut_on_different_chrom_copy_name = db.random_t_name_w_prefix('with_mutations_on_diff_chrom_copy')
# db.table_mutations_on_different_chrom_copies(samples_having_mut_on_different_chrom_copy_name, 'dw', mut1, mut2_fingerprint)
# db.print_table_named(samples_having_mut_on_different_chrom_copy_name, 'dw')
samples_having_mut_on_different_chrom_copy_name = 'insert name here'


# MAKES AND OF CHARACTERISTICS BY INDIVIDUAL
# regions_from_common_item_id_name = db.random_t_name_w_prefix('regions_common_item_id')
# db.take_regions_of_common_individuals(regions_from_common_item_id_name, 'dw',
#                                       [samples_having_all_mutations_name,
#                                        samples_having_all_mutations_on_same_chrom_copy_name,
#                                        samples_having_mut_on_different_chrom_copy_name],
#                                       ['dw', 'dw', 'dw'])
# db.print_table_named(regions_from_common_item_id_name, 'dw')
regions_from_common_item_id_name = 'regions_common_item_id_2020_03_05_03_26_01'

regions_from_common_item_id_name = db.random_t_name_w_prefix('i')
db.take_regions_of_common_individuals(regions_from_common_item_id_name, 'dw',
                                      ['a',
                                       'b',
                                       'c'],
                                      ['dw', 'dw', 'dw'])



# COUNT OF SAMPLES BY DIMENSION
# count_result = db.count_samples_by_dimensions(view_of_samples_name, regions_from_common_item_id_name, 'dw')
# db.print_query_result(count_result)


# MUTATION FREQUENCY BY DIMENSION
# freq_result = db.mutation_frequency_by_dimensions(view_of_samples_name, regions_from_common_item_id_name, 'dw')
# print('row count {}'.format(freq_result.rowcount))
# db.print_query_result(freq_result)


# MUTATIONS IN REGION
# regions_in_interval_t_name = db.random_t_name_w_prefix('mut_in_intrv')
# db.view_of_mutations_in_interval_or_type(regions_in_interval_t_name, 'dw', 1, 0, 10176, None)
# db.view_of_mutations_in_interval_or_type(regions_in_interval_t_name, 'dw', None, None, None, mut_type=['SNP', 'CNV'])
# db.view_of_mutations_in_interval_or_type(regions_in_interval_t_name, 'dw', 1, 0, 10176, mut_type=['SNP', 'CNV', 'INS'])


if db is not None:
    db.disconnect()