--2.4
--Verificar a receita da loja com o código 32 no período igual ou superior a 01/04/2022
--Enuciado pede a loja 32, mas como a loja 32 não existe mantive o código 21 apenas para testes na query
with lojas as (
select id_loja, cod_loja, nm_loja from dim_loja  where cod_loja in(21)
),
periodo as (
select id_tempo from dim_tempo where dt >= '2021-04-01'
)
select 
	lojas.nm_loja as nm_loja,
	sum(receita_venda) as receita_venda
from 
	ft_vendas 
join 
	periodo on (periodo.id_tempo = ft_vendas.id_tempo)
join
	lojas   on (lojas.id_loja = ft_vendas.id_loja)
group by
	lojas.cod_loja,
	lojas.nm_loja