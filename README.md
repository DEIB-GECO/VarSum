# data_summarization_1KGP
This tool is an example of application of the genomic data repository maintained by Politecnico di Milano - http://www.bioinformatics.deib.polimi.it/geco/ - and it is aimed at describing genomic variation data sources, with a particular focus on the 1000 Genomes Project. This source contains +88 millions variants from ⁓2.5 thousands individauls, who were self-declared healthy and sampled from 26 populations worldwide, including the continents America, Europe, East Asia, South Asia and Africa.

The researcher who is interested in identifying a suitable control population for an experiment, can obtain through this tool some summary statistics describing a set of individuals, including:
- Number of individuals
- Number of variants
- Most frequent variants
- Rarest variants
- Average mutation frequency.

These measures are described as distributions over the attributes of the individuals composing the population under study, i.e.:
- Continent of provenance
- Country of provenance
- Gender 
- Source of the DNA sample.

Where each of the above characteristics can be fixed in order to restrict the set of individuals of interest to the ones having certain characteristics. 

The user can also impose further constraints on the target population, like the presence of some specific variants and their location (same or different chromosome copy), the presence of variants in a precise genomic area, and the inclusion of only some mutation kinds.

## Abbreviations and terms
As a reference, below are listed some abbreviations in use across the 1000 Genomes data.

#### Country and Continent of provenance

| __population__  |  population description  |  __super_population__  |
|--- |--- |--- |
| CHB  |  Han Chinese in Beijing, China  |  EAS  |
| JPT  |  Japanese in Tokyo, Japan  |  EAS  |
| CHS  |  Southern Han Chinese  |  EAS  |
| CDX  |  Chinese Dai in Xishuangbanna, China  |  EAS  |
| KHV  |  Kinh in Ho Chi Minh City, Vietnam  |  EAS  |
| CEU  |  Utah Residents (CEPH) with Northern and Western European Ancestry  |  EUR  |
| TSI  |  Toscani in Italia  |  EUR  |
| FIN  |  Finnish in Finland  |  EUR  |
| GBR  |  British in England and Scotland  |  EUR  |
| IBS  |  Iberian Population in Spain  |  EUR  |
| YRI  |  Yoruba in Ibadan, Nigeria  |  AFR  |
| LWK  |  Luhya in Webuye, Kenya  |  AFR  |
| GWD  |  Gambian in Western Divisions in the Gambia  |  AFR  |
| MSL  |  Mende in Sierra Leone  |  AFR  |
| ESN  |  Esan in Nigeria  |  AFR  |
| ASW  |  Americans of African Ancestry in SW USA  |  AFR  |
| ACB  |  African Caribbeans in Barbados  |  AFR  |
| MXL  |  Mexican Ancestry from Los Angeles USA  |  AMR  |
| PUR  |  Puerto Ricans from Puerto Rico  |  AMR  |
| CLM  |  Colombians from Medellin, Colombia  |  AMR  |
| PEL  |  Peruvians from Lima, Peru  |  AMR  |
| GIH  |  Gujarati Indian from Houston, Texas  |  SAS  |
| PJL  |  Punjabi from Lahore, Pakistan  |  SAS  |
| BEB  |  Bengali from Bangladesh  |  SAS  |
| STU  |  Sri Lankan Tamil from the UK  |  SAS  |
| ITU  |  Indian Telugu from the UK  |  SAS  |

#### Source of the DNA sample
Common values for the parameter __dna_source__ are:
- lcl (Lymphoblastoid Cell lines)
- blood