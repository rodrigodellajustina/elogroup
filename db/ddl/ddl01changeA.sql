-- Eliminar estruturas;
drop table if exists ft_vendas;
drop table if exists dim_produto;
drop table if exists dim_tempo;
drop table if exists dim_loja;
drop table if exists dim_cliente;


-- cria estruturas do modelo dimensional;
create table dim_produto(
   id_produto integer not null, 
   cod_produto integer, 
   nm_produto text,
   secao text,
   grupo text,
   subgrupo text
);

-- cria estruturas de dimensão de tempo;
create table dim_tempo(
   id_tempo integer not null,
   ano integer not null,
   mes integer not null,
   dia integer not null,
   dt date not null
);

create table dim_loja(
   id_loja integer not null,
   cod_loja integer not null,
   nm_loja text
);

create table dim_cliente(
   id_cliente integer not null,
   estado_civil text,
   sexo text,
   bairro text
);

create table ft_vendas(
   id_loja integer not null,
   id_produto integer not null,
   id_cliente integer not null,
   id_tempo integer not null,
   qtde_vendida integer not null,
   receita_venda numeric not null
);

-- criação de chaves primárias no modelo
alter table dim_produto add primary key (id_produto);
alter table dim_tempo add primary key (id_tempo);
alter table dim_loja add primary key (id_loja);
alter table dim_cliente add primary key (id_cliente);
alter table ft_vendas add primary key (id_loja, id_produto, id_cliente, id_tempo);

-- criação das chaves estrangeiras
alter table ft_vendas add foreign key (id_produto) references dim_produto(id_produto) on delete cascade;
alter table ft_vendas add foreign key (id_tempo) references dim_tempo(id_tempo) on delete cascade;
alter table ft_vendas add foreign key (id_loja) references dim_loja(id_loja) on delete cascade;
alter table ft_vendas add foreign key (id_cliente) references dim_cliente(id_cliente) on delete cascade;

select * from dim_cliente
select * from dim_loja 
select * from dim_produto 
select * from dim_tempo 
select * from ft_vendas