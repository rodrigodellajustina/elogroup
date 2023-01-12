--2.3
--Buscar os produtos com código 55,120,142 e que tiveram receita maior que R$120,00;
select 
	dim_produto.cod_produto,
	dim_produto.nm_produto
from 
	dim_produto 
where 
	cod_produto in (55, 120, 142) and 
	exists(select 1 from ft_vendas where ft_vendas.id_produto = dim_produto.id_produto and ft_vendas.receita_venda > 120)