# data_summarization_1KGP
This tool is an example of application of the genomic data repository maintained by Politecnico di Milano - http://www.bioinformatics.deib.polimi.it/geco/ - and it is specifically aimed at describing the data source 1000 Genomes Project. This source contains +88 millions mutations from ⁓2.5 thousands individauls, who were self-declared healthy and sampled from 26 populations worldwide, including the continents America, Europe, East Asia, South Asia and Africa.

The researcher who is interested in identifying a suitable control population for an experiment, can obtain throught this tool some summary statistics describing a set of individuals, including:
- Numerosity of individuals
- Numerosity of mutations
- Most frequent mutations
- Most rare mutations
- Avergae mutation frequency
These measures are described as distributions over the attributes of the individuals composing the population under study, i.e.:
- Continent of provenance
- Country of provenance
- Gender 
- Source of the DNA sample
Where each of the above characteristics can be fixed in order to restrict the set of individuals of interest to the ones having certain characteristics. 

The user can also impose further constraints on the population, like the presence of some specific mutations and their location (same or different chromosome copy), the presence of mutations in a precise genomic area, and the inclusion of only some mutation kinds.
