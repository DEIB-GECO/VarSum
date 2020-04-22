CREATE OR REPLACE FUNCTION dw.kgenomes_ethnicity(super_population varchar)
RETURNS varchar AS $$
declare res varchar;
begin
    if super_population = 'AMR' then
        res := 'latin american';
    elsif super_population = 'EUR' then
        res := 'white';
    elsif super_population = 'AFR' then
        res := 'black or african american';
    elsif super_population = 'SAS' or super_population = 'EAS' then
        res := 'asian';
    else
        res := 'not reported';
	end if;
	return res;
-- this is a workaround to assign ethnicities to 1000 genomes. The correct way would be
-- to rerun metadata manager and assign the values from them, but the need for ethnicity
-- in 1000 genomes is a need that arrived too late to re-run metadata manager (it takes 2 weeks).
end;
$$ language plpgsql;
