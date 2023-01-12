-- 2.
-- Quantidade vendida no período entre 01/04/2020 a 01/04/2021;

select 
	sum(ft_vendas.qtde_vendida) as qtde_vendida 
from 
	public.ft_vendas  
where 
	ft_vendas.id_tempo in (
				     select 
					id_tempo 
				     from 
					dim_tempo
				     where 
					dt between '2020-04-01' and '2021-04-01'
			      )