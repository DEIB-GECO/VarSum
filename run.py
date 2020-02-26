from datetime import datetime
from database import DBConnection
from mutation import Mutation
import sys

db = DBConnection(sys.argv[1], sys.argv[2])

# FILTER BY METADATA
# view_of_samples_name = 'sample_view' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.view_of_samples_with_metadata(view_of_samples_name, 'female', 'true', None, 'hg19', ['PJL'], None)
# db.print_table_named(view_of_samples_name, 'dw')
view_of_samples_name = 'sample_view_2020_02_19_16_58_51'

mut1 = Mutation(_id='rs367896724')  # these two are on different chromosome copies
mut2 = Mutation(_id='rs555500075')
mut3_al1 = Mutation(_id='rs367896724')  # these two are on same chromosome copy
mut4_al1 = Mutation(_id='rs376342519')

# HAVING MUTATIONS 3 AND 4
# samples_having_all_mutations_name = 'samples_with_mut3_and_4' + datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.with_all_mutations(view_of_samples_name, samples_having_all_mutations_name, 'dw', mut3_al1, mut4_al1)
# db.print_table_named(samples_having_all_mutations_name, 'dw')
samples_having_all_mutations_name = 'samples_with_mut3_and_4_2020_02_26_23_14_01'

# HAVING MUTATIONS 3 AND 4 ON SAME CHROMOSOME COPY
# samples_having_all_mutations_on_same_chrom_copy_name = 'with_mutations_on_same_chrom_copy' + \
#                                                        datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.with_all_mutations_on_same_chrom_copy(samples_having_all_mutations_name, 'dw',
#                                          samples_having_all_mutations_on_same_chrom_copy_name, 'dw', mut3_al1, mut4_al1)
# db.print_table_named(samples_having_all_mutations_on_same_chrom_copy_name, 'dw')
samples_having_all_mutations_on_same_chrom_copy_name = 'with_mutations_on_same_chrom_copy_2020_02_26_23_39_24'

# HAVING MUTATIONS 1 AND 2 ON DIFFERENT CHROMOSOME COPIES
# samples_having_mut_on_different_chrom_copy_name = 'with_mutations_on_diff_chrom_copy' + \
#                                                        datetime.now().strftime('_%Y_%m_%d_%H_%M_%S')
# db.with_mutations_on_different_chrom_copies('samples_region_view_2020_02_25_21_43_34', 'dw',
#                                          samples_having_mut_on_different_chrom_copy_name, 'dw', mut1, mut2)
# db.print_table_named(samples_having_mut_on_different_chrom_copy_name, 'dw')
samples_having_mut_on_different_chrom_copy_name = 'with_mutations_on_diff_chrom_copy_2020_02_25_23_30_44'

# COUNT OF SAMPLES BY DIMENSION
# count_result = db.count_samples_by_dimensions('samples_with_metadata')
# db.print_query_result(count_result)


# MUTATION FREQUENCY BY DIMENSION
# freq_result = db.mutation_frequency_by_dimensions('samples_for_counting', 'region_for_counting_lim', 'dw')
# db.print_query_result(freq_result)
