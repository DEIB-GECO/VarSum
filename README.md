# VarSum (http://gmql.eu/popstudy)

This API returns summary statistics on user-defined populations and their v
ariants, using the genomic data repository maintained by the [genomic computing group 
(GeCo) at Politecnico di Milano](http://www.bioinformatics.deib.polimi.it/geco/). 

Available data include gene annotations, germline variants from the 1000 
Genomes Project (~88 millions) and somatic mutations collected by 
The Cancer Genomes Atlas Program (~4 millions)

Through this tool, it is possible to get summary statistics describing 
a population, including:
- Number of individuals grouped by sub-populations.
- Most frequent variants
- Rarest variants
- Average mutation frequency in multiple population subgroups.

Subgroups are identified by distributing the population according to:
- Continent of provenance
- Country of provenance
- Ethnicity
- Gender
- Health status  
- Disease
- Source of the DNA sample.

Where each of the above characteristics can be fixed in order to restrict the 
set of individuals of interest to the ones having certain characteristics. 

The user can also impose further constraints on the target population, like 
the presence/absence of specific variants and their relative position (same or different 
chromosome copy) and also impose the presence of variants in a precise 
genomic area or gene.



    
## Overview of the interface

Requests and responses are exchanged  as JSON formatted messages. 

### Response format

In general, this API returns a table-like data structure, represented as
   
```yaml
{
  "columns": ["COLUMN_NAME_1", "COLUMN_NAME_2", ...],
  "rows": [
      ["1st_value_of_ROW_1", "2nd_value_of_ROW_1", ...],
      ["1st_value_of_ROW_2", "2nd_value_of_ROW_2", ...],
      ...
    ],
  "notice": "optional message", "another optional message", ...
}
```
where `columns` and `rows` contain the table headers and rows respectively. 
A field `notice` may be optionally present to inform of characteristics of the requested data that can lead to unexpected results or to provide complementary details. This happens when one or more of the following conditions is verified:

- A part of the population selected by the user cannot be used to produce the result because of unkown/unrecognized cathegorical values. 
	
	For example, when computing the frequency of a variant falling in sex chromosome, individuals with unknown gender will be automatically excluded from the population selected by the user. That's because the gender information is necessary to compute the total number of alleles in that locus of the considered population.  
- The population identified by the request parameters is empty because no individual satisfies the requirements in the available data sources.
- The popuation identified by the request parameters originates from multiple data sources and contains both germline variants and somatic mutations. 
- A request made to endpoints `most_common_variants` or `rarest_variants` includes the optional parameter 
	```yaml
	filter_output: {
		time_estimate_only: true
	}
	```
Only the endpoint `values` breaks the above rule, as it returns a mapping from values to the available data source names. For example, a request asking the values of the attribute "gender" returns:
```yaml
# response of gmql.eu/popstudy/values/gender at the time of writing
{
  "female": [
    "1000Genomes",
    "TCGA"
  ],
  "male": [
    "1000Genomes",
    "TCGA"
  ],
  "not_reported": [
    "TCGA"
  ]
}
``` 

### Request parameters for studying a population
This paragraph applies to endpoints `donor_grouping`, `variant_grouping`, `most_common_variants`, `rarest_variants` and `download_donors`. 
To select a population, the user can express any combination of metadata and region constraints, and also restrict the data sources to use. The skeleton of a request body making use of all the possible constraints, looks like the following:
```yaml
{
  "having_meta": {
    "gender": ...,				# the required gender
    "dna_source": ..., 			# the originating tissue type 
    "assembly": ...,			# 'GRCh38' or 'hg19'
    "ethnicity": ...,			# a list of ethnicities
    "super_population": ...,	# a list of super populations
    "population": ...,			# a list of populations
    "healthy": ...,				# true or false
    "disease": ... 				# a disease name
  },
  "having_variants": {
    "with": ...,				# a list of variants carried by every individual 
    "without": ...,				# a list of variants not carried by any individual
    "on_same_chrom_copy": ...,	# a list of variants of a single chromosome and all carried by each individual on either the paternal or maternal chromosome 
    "on_diff_chrom_copy": ...,	# two variants of a single chromosome and carried by each individual one on the paternal and the other on the maternal chromosome (order is irrelevant)
    "in": ...,					# a mutated genomic region 
    "in_cell_type": ...			# 'somatic' or 'germline'
  },
  "source": ...					# a list of sources from the ones available
}
```
- To leave unconstrained a parameter, just remove it from the above example.
- Some parameters accept multiple values grouped inside a list, instead others can use only one value.
- For a list of the values accepted by parameters in `having_meta`, you can call the endpoint `values` and pass the parameter name.
- Each of the endpoints can require additional parameters to perform an action. If it is a "grouping" endpoint, it requires a `group_by` to specify one or more metadata categories; if it studies the frequency of a single variant, it requires a `target_variant`; lastly, endpoints `most_common_variants` and  `rarest_variants` offer the possibilty to partition the result table with `filter_output`.

### Request parameters for exploring a genomic region
The endpoint `variants_in_region`  lists the variants falling in a region of interest for a population . The request body is an extension of the previous, with small changes.
```yaml
{
  # you can refer to any annotated region with the parameters
  "name": ...,
  "type": ...,
  "ensemble_id": ...,
  # or you can define your region of interest with
  "chrom": ...,
  "start": ...,
  "stop": ...,
  "strand": ...,
  # then, you can limit the set of variants to those carried by the population defined as (only "assembly" is mandatory):
  "of": {							
    "gender": ...,				# the required gender
    "dna_source": ..., 			# the originating tissue type 
    "assembly": ...,			# 'GRCh38' or 'hg19'
    "ethnicity": ...,			# a list of ethnicities
    "super_population": ...,	# a list of super populations
    "population": ...,			# a list of populations
    "healthy": ...,				# true or false
    "disease": ...,				# a disease name
    "having_variants": {
      "with": ...,				# a list of variants carried by every individual 
      "without": ...,				# a list of variants not carried by any individual
      "on_same_chrom_copy": ...,	# a list of variants of a single chromosome and all carried by each individual on either the paternal or maternal chromosome 
      "on_diff_chrom_copy": ...,	# two variants of a single chromosome and carried by each individual one on the paternal and the other on the maternal chromosome (order is irrelevant)
      "in": ...,					# a mutated genomic region 
      "in_cell_type": ...			# 'somatic' or 'germline'
    },
  "source": ...					# a list of sources from the ones available
}
```

Finally, the endpoint `annotate` tells you the genes that overlap (even not completely) with a given variant or genomic interval.  A simplified request body is exemplified below. 
```yaml
{
  "assembly": ..., 
  # you can specify a variant by typing its:
  # - dbSNP id
  "id": ...,
  # - or its genomic properties (in 0-based coordinates):
  "chrom": ...,
  "start": ...,
  "ref": ...,
  "alt": ...
  # alternatively, you can instead type a genomic interval of interest:
  "chrom": ...,
  "start": ...,
  "stop": ...,
  "strand": ... 
}
```

## Additional Resources
   
   - [Populations, ethncicity and other abbreviations used in the data sources](https://github.com/DEIB-GECO/VarSum#abbreviations-and-terms)
   - [Examples and Demonstrative applications (use cases)](https://github.com/DEIB-GECO/VarSum/tree/master/demo#varsum-httpgmqleupopstudy---examples--applications) as Python Notebooks on GitHub or Google Colab notebooks  
   - [GitHub project repository](https://github.com/DEIB-GECO/VarSum)
   
## How to

- [(video) Exploring the documentation ](https://polimi365-my.sharepoint.com/:v:/g/personal/10435046_polimi_it/ESEAJhaYj-FHh3AfcVYkp0wBDcI7djdLQm_twsIdjSpdTw?e=qMKfkY)
- [(video) Try the API by yourself](https://polimi365-my.sharepoint.com/:v:/g/personal/10435046_polimi_it/ETN9_W7Z9xtMpg4l0UwfzTwBcDKvFkJ1eUKxGOcEvQGzIw?e=R9YFnZ)

## Abbreviations and terms
As a reference, below are listed some abbreviations in use.

#### Ethnicity - Country and Continent of provenance

| __population__  |  population description  |  __super_population__  | __ethnicity__
|--- |--- |--- |--- |
| CHB  |  Han Chinese in Beijing, China  |  EAS  |  asian  |
| JPT  |  Japanese in Tokyo, Japan  |  EAS  |  asian  | 
| CHS  |  Southern Han Chinese  |  EAS  |  asian  | 
| CDX  |  Chinese Dai in Xishuangbanna, China  |  EAS  |  asian  | 
| KHV  |  Kinh in Ho Chi Minh City, Vietnam  |  EAS  |  asian  | 
| GIH  |  Gujarati Indian from Houston, Texas  |  SAS  |  asian  | 
| PJL  |  Punjabi from Lahore, Pakistan  |  SAS  |  asian  | 
| BEB  |  Bengali from Bangladesh  |  SAS  |  asian  | 
| STU  |  Sri Lankan Tamil from the UK  |  SAS  |  asian  | 
| ITU  |  Indian Telugu from the UK  |  SAS  |  asian  | 
| CEU  |  Utah Residents (CEPH) with Northern and Western European Ancestry  |  EUR  |  white  |
| TSI  |  Toscani in Italia  |  EUR  |  white  |
| FIN  |  Finnish in Finland  |  EUR  |  white  |
| GBR  |  British in England and Scotland  |  EUR  |  white  |
| IBS  |  Iberian Population in Spain  |  EUR  |  white  |
| YRI  |  Yoruba in Ibadan, Nigeria  |  AFR  |  black or african american  |
| LWK  |  Luhya in Webuye, Kenya  |  AFR  |  black or african american  |
| GWD  |  Gambian in Western Divisions in the Gambia  |  AFR  |  black or african american  |
| MSL  |  Mende in Sierra Leone  |  AFR  |  black or african american  |
| ESN  |  Esan in Nigeria  |  AFR  |  black or african american  |
| ASW  |  Americans of African Ancestry in SW USA  |  AFR  |  black or african american  |
| ACB  |  African Caribbeans in Barbados  |  AFR  |  black or african american  |
| MXL  |  Mexican Ancestry from Los Angeles USA  |  AMR  |  latin american  |
| PUR  |  Puerto Ricans from Puerto Rico  |  AMR  |  latin american  |
| CLM  |  Colombians from Medellin, Colombia  |  AMR  |  latin american  |
| PEL  |  Peruvians from Lima, Peru  |  AMR  |  latin american  |

Other ethnies do not have a matching population or super_population attribute:
 - native hawaiian or other pacific islander
 - american indian or alaska native

#### Source of the DNA sample
Common values for the parameter __dna_source__ are:
- lcl (Lymphoblastoid Cell lines)
- blood
