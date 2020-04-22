CREATE MATERIALIZED VIEW dw.genomes_metadata
TABLESPACE default_ts
AS SELECT x.donor_id,
    x.donor_source_id,
    x.item_id,
    x.item_source_id,
    x.file_name,
    x.local_url,
    lower(x.assembly::text) AS assembly,
    x.gender,
    x.health_status,
    x.population,
    p1.value AS super_population,
    p2.value AS dna_source
   FROM ( SELECT biosample.donor_id,
            donor.donor_source_id,
            item.item_id,
            item.item_source_id,
            item.file_name,
            item.local_url,
            dataset.assembly,
            donor.gender,
            biosample.is_healthy AS health_status,
            donor.ethnicity AS population
           FROM dw.item
             JOIN dataset USING (dataset_id)
             JOIN replicate2item USING (item_id)
             JOIN dw.replicate USING (replicate_id)
             JOIN biosample USING (biosample_id)
             JOIN donor USING (donor_id)
          WHERE dataset.dataset_name::text ~~* '%1000GENOMES%'::text OR dataset.dataset_name::text ~~* '%TCGA_somatic_mutation%'::text OR dataset.dataset_name::text ~~* '%TCGA_dnaseq'::text) x
     LEFT JOIN pair p1 ON x.item_id = p1.item_id AND p1.key::text = 'super_population'::text
     LEFT JOIN pair p2 ON x.item_id = p2.item_id AND p2.key::text = 'dna_source_from_coriell'::text
WITH DATA;