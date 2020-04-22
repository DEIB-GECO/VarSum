# data_summarization_1KGP
This tool is an example of application of the genomic data repository maintained by Politecnico di Milano - http://www.bioinformatics.deib.polimi.it/geco/ - and it is aimed at describing genomic variation data sources, with a particular focus on the 1000 Genomes Project. This source contains +88 millions variants from ⁓2.5 thousands individauls, who were self-declared healthy and sampled from 26 populations worldwide, including the continents America, Europe, East Asia, South Asia and Africa.

The researcher who is interested in identifying a suitable control population for an experiment, can obtain through this tool some summary statistics describing a set of individuals, including:
- Number of individuals
- Most frequent variants
- Rarest variants
- Average mutation frequency.

These measures are described as distributions over the attributes of the individuals composing the population under study, i.e.:
- Continent of provenance
- Country of provenance
- Ethnicity
- Gender
- Health status
- Source of the DNA sample.

Where each of the above characteristics can be fixed in order to restrict the set of individuals of interest to the ones having certain characteristics. 

The user can also impose further constraints on the target population, like the presence of some specific variants and their location (same or different chromosome copy) and the presence of variants in a precise genomic area or gene.

## Abbreviations and terms
As a reference, below are listed some abbreviations in use across the 1000 Genomes data.

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
