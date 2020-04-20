CREATE OR REPLACE FUNCTION rr.mut_frequency_new_hg19(occurrence bigint, males integer, females integer, chrom integer, start_ bigint)
RETURNS numeric AS $$
declare total_alleles integer;
declare res numeric;
begin
	if occurrence > 0 then
		-- calculate total alleles
		if chrom<23 then
			total_alleles := (males+females)*2;
		elsif chrom=23 then
			if start_ < 2699520 or start_ > 154931044 then
				total_alleles := (males+females)*2;
			else
				total_alleles := males + females*2 ;
			end if;
		elsif chrom=24 then
			total_alleles := males;
		else
			total_alleles := males+females;
		end if;
		-- calculate result
		if total_alleles > 0 then
			res = occurrence::numeric / total_alleles;
		else
			res := 0;
		end if;
	else
		res := 0;
	end if;
	return res;
-- chromosomes X and Y share the leading and trailing regions (called pseudoautosomal regions)
-- so in that regions, males have two alleles as females have.
-- grch38 regions differ slightly from hg19 ones.
-- note that chromosome 25 is mitochondrial dna
end;
$$ language plpgsql;
