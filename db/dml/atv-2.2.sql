--3.
--Quantidade de clientes únicos do sexo feminino e estado civil divorciada;
select count(*) as qtd_fem_divociado from dim_cliente 
where lower(sexo) = 'feminino' and lower(estado_civil) = 'divorciada'
-- 0

-- porém Divorciado possui 1 resultado
select count(*) as qtd_fem_divorciado from dim_cliente 
where lower(sexo) = 'feminino' and lower(estado_civil) = 'divorciado'


-- Complemento
-- totalizando por estsado civil com a clausula feminino
select estado_civil, count(*) as qtd_fem_divorciado from dim_cliente 
where lower(sexo) = 'feminino'
group by estado_civil
order by qtd_fem_divorciado desc