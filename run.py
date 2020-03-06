from datetime import datetime
from database import DBConnection
from mutation import Mutation
import sys

db_user = sys.argv[1]
db_password = sys.argv[2]
db = None

if __name__ == '__main__':
    db = DBConnection(db_user, db_password)
    db.log_sql_commands = True

# FILTER BY METADATA
# view_of_samples_name = 'sample_view' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.view_of_samples_with_metadata(view_of_samples_name, health_status='true', assembly='hg19', super_population=['EAS'])
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
# db.table_with_all_of_mutations(None, samples_having_all_mutations_name, 'dw', mut3_al1, mut4_al1)
# db.print_table_named(samples_having_all_mutations_name, 'dw')
samples_having_all_mutations_name = 'samples_with_mut3_and_4_2020_03_04_20_17_43'

# HAVING MUTATIONS 3 AND 4 ON SAME CHROMOSOME COPY
# samples_having_all_mutations_on_same_chrom_copy_name = 'with_mutations_on_same_chrom_copy' + \
#                                                        datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.table_mutations_on_same_chrom_copy(None, 'dw',
#                                          samples_having_all_mutations_on_same_chrom_copy_name, 'dw', mut3_al1, mut4_al1)
# db.print_table_named(samples_having_all_mutations_on_same_chrom_copy_name, 'dw')
samples_having_all_mutations_on_same_chrom_copy_name = 'with_mutations_on_same_chrom_copy_2020_03_04_20_37_09'

# HAVING MUTATIONS 1 AND 2 ON DIFFERENT CHROMOSOME COPIES
# samples_having_mut_on_different_chrom_copy_name = 'with_mutations_on_diff_chrom_copy' + \
#                                                        datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.table_mutations_on_different_chrom_copies(None, 'dw',
#                                             samples_having_mut_on_different_chrom_copy_name, 'dw', mut1, mut2_fingerprint)
# db.print_table_named(samples_having_mut_on_different_chrom_copy_name, 'dw')
samples_having_mut_on_different_chrom_copy_name = 'with_mutations_on_diff_chrom_copy_2020_03_04_20_21_33'

# MAKES AND OF CHARACTERISTICS BY INDIVIDUAL
# regions_from_common_item_id_name = 'regions_common_item_id' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.take_regions_of_common_individuals(regions_from_common_item_id_name, 'dw',
#                                       [samples_having_all_mutations_name,
#                                        samples_having_all_mutations_on_same_chrom_copy_name,
#                                        samples_having_mut_on_different_chrom_copy_name],
#                                       ['dw', 'dw', 'dw'])
# db.print_table_named(regions_from_common_item_id_name, 'dw')
regions_from_common_item_id_name = 'regions_common_item_id_2020_03_05_03_26_01'


# COUNT OF SAMPLES BY DIMENSION
# count_result = db.count_samples_by_dimensions(view_of_samples_name, regions_from_common_item_id_name, 'dw')
# db.print_query_result(count_result)


# MUTATION FREQUENCY BY DIMENSION
# freq_result = db.mutation_frequency_by_dimensions(view_of_samples_name, regions_from_common_item_id_name, 'dw')
# print('row count {}'.format(freq_result.rowcount))
# db.print_query_result(freq_result)


if db is not None:
    db.disconnect()
