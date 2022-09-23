

-- select *
-- from Dataton22_Base_conteo_prototipo
-- ;

drop table if exists proceso.Dataton22_tabla_Categorias purge
;
create table  proceso.Dataton22_tabla_Categorias stored as parquet as
with aux as(
select *,
(case
when (count_name_tittle*2 + count_name_text) > 0 then "Cliente"
when (count_sect_tittle*2 + count_sect_text) > 0 then "Sector"
when (count_ciiu_tittle*2 + count_ciiu_text) > 0 then "Sector" 
else "No_aplica"
end) as categoria_1,

if(count_macroeconomicas_tittle > 0 or count_macroeconomicas_text > 0, 1, 0) as term_macroeconomicos,
if(count_sostenibilidad_tittle > 0 or count_sostenibilidad_text > 0, 1, 0) as term_sostenibilidad,
if(count_innovacion_tittle > 0 or count_innovacion_text > 0, 1, 0) as term_innovacion,
if(count_regulaciones_tittle > 0 or count_regulaciones_text > 0, 1, 0) as term_regulaciones,
if(count_alianzas_tittle > 0 or count_alianzas_text > 0, 1, 0) as term_alianzas,
if(count_reputacion_tittle > 0 or count_reputacion_text > 0, 1, 0) as term_reputacion,

(count_macroeconomicas_tittle*3 + count_macroeconomicas_text) as ind_macroeconomico,
(count_sostenibilidad_tittle*3 + count_sostenibilidad_text) as ind_sostenibilidad,
(count_innovacion_tittle*3 + count_innovacion_text) as ind_innovacion,
(count_regulaciones_tittle*3 + count_regulaciones_text) as ind_regulaciones,
(count_alianzas_tittle*3 + count_alianzas_text) as ind_alianzas,
(count_reputacion_tittle*3 + count_reputacion_text) as ind_reputacion

from Dataton22_Base_conteo_prototipo
),

aux2 as(
select *,
(case
WHEN ind_sostenibilidad > 0 and ind_sostenibilidad >= greatest(ind_macroeconomico, ind_innovacion, ind_regulaciones, ind_alianzas, ind_reputacion) then "Sostenibilidad"
WHEN ind_macroeconomico > 0 and ind_macroeconomico >= greatest(ind_sostenibilidad, ind_innovacion, ind_regulaciones, ind_alianzas, ind_reputacion) then "Macroeconomia"
WHEN ind_innovacion > 0 and ind_innovacion >= greatest(ind_macroeconomico, ind_sostenibilidad, ind_regulaciones, ind_alianzas, ind_reputacion) then "Innovacion"
WHEN ind_regulaciones > 0 and ind_regulaciones >= greatest(ind_macroeconomico, ind_innovacion, ind_sostenibilidad, ind_alianzas, ind_reputacion) then "Regulaciones"
WHEN ind_alianzas > 0 and ind_alianzas >= greatest(ind_macroeconomico, ind_innovacion, ind_regulaciones, ind_sostenibilidad, ind_reputacion) then "Alianzas"
WHEN ind_reputacion > 0 and ind_reputacion >= greatest(ind_macroeconomico, ind_innovacion, ind_regulaciones, ind_alianzas, ind_sostenibilidad) then "Reputacion"
else "Ninguna"
end) as categoria_2
from aux 
)

select *,
(case
when categoria_1 = "No_aplica" then "Descartable"
when categoria_1 != "No_aplica" and categoria_2 = "Ninguna" then "Otra"
when categoria_1 !=  "No_aplica" and categoria_2 != "Ninguna" then categoria_2
end) as categoria_2_f

--if(categoria_1 = categoria_2 and categoria_1 = "ninguna", "descartable", concat_ws('|', categoria_1, categoria_2)) as categoria_final
from aux2
;

compute stats proceso.Dataton22_tabla_Categorias
;

--------------------------------------------
-------- IDEAS INSPIRACIONALES -------------
--------------------------------------------
--- 1. crear mejores indicadores de texto con categoria -- modelos de ML 
--- 2. tener una mejor logica que solo un case when para elegir la categoria adecuada --- modelos ML

----------------------------------------------
----------------------------------------------

-- select *
-- from proceso.Dataton22_tabla_Categorias
-- --where categoria_1 = 'ninguna' AND categoria_2 = 'macroeconomia' AND categoria_2_f = 'descartable'
-- --where categoria_1 = 'cliente' AND categoria_2 = 'sostenibilidad' AND categoria_2_f = 'sostenibilidad'
-- --where categoria_1 = 'ninguna' AND categoria_2 = 'macroeconomia' AND categoria_2_f = 'descartable'
-- where categoria_1 = 'cliente' AND categoria_2 = 'ninguna' AND categoria_2_f = 'otra'
-- limit 100
--;


-- select categoria_1, categoria_2, categoria_2_f, count(1) cnt, count(distinct nit)
-- from proceso.Dataton22_tabla_Categorias
 --where categoria_2_f != 'descartable'
-- group by 1, 2, 3
-- limit 100
-- ;


-- select count(distinct nit)
-- from proceso.Dataton22_tabla_Categorias
-- where categoria_2_f != 'descartable'
-- --group by 1, 2, 3
--limit 100
--;


-- select distinct nombre
-- from proceso.Dataton22_tabla_Categorias
-- ;

----------------------------------------------
------- Entregable 1--------------------------
----------------------------------------------

drop table if exists Dataton22_entregable_1 purge
;
--explain
create table  Dataton22_entregable_1 stored as parquet as

select 
distinct
'dataton_owners' as nombre_equipo,
nit,
new_id,
categoria_1 as participacion,
categoria_2_f as categoria
from proceso.Dataton22_tabla_Categorias
;

compute stats Dataton22_entregable_1
;

----------------------------------------------
------- Fiabilidad fuente y ind_reciente---------------------
----------------------------------------------

drop table if exists proceso.Dataton22_tabla_fiabilidad_reciente purge
;
create table  proceso.Dataton22_tabla_fiabilidad_reciente stored as parquet as
with aux as(
select 
fuente,
count(1) as cnt_noticias
from proceso.Dataton22_tabla_Categorias
group by 1
),

aux2 as (
select *,
percent_rank() over (order by cnt_noticias) as rank_fiabilidad_fuente
from aux
),

aux3 as(
select new_id,
news_final_date,
to_timestamp(regexp_replace(news_final_date, '-', ''),'yyyyMMdd') as fecha_new,
datediff(now(), to_timestamp(regexp_replace(news_final_date, '-', ''),'yyyyMMdd')) as dif_days
from Dataton22_Base_conteo_prototipo
),

aux4 as(
select  new_id,
1 - (dif_days / (max(dif_days) over())) as ind_reciente
from aux3
)

select 
t1.nit,
t1.nombre,
t1.new_id,
t1.fuente,
t1.news_url_absolute,
t1.news_init_date,
t1.news_final_date,
to_timestamp(regexp_replace(news_final_date, '-', ''),'yyyyMMdd') as fecha_new,
t1.news_title,

percent_rank() over (order by ind_macroeconomico) as rank_ind_macroeconomico,
percent_rank() over (order by ind_sostenibilidad) as rank_ind_sostenibilidad,
percent_rank() over (order by ind_innovacion) as rank_ind_innovacion,
percent_rank() over (order by ind_regulaciones) as rank_ind_regulaciones,
percent_rank() over (order by ind_alianzas) as rank_ind_alianzas,
percent_rank() over (order by ind_reputacion) as rank_ind_reputacion,

t1.categoria_1 as participacion,
t1.categoria_2_f as categoria,

t2.cnt_noticias,
t2.rank_fiabilidad_fuente,
t3.ind_reciente

from proceso.Dataton22_tabla_Categorias t1
left join aux2 t2 on (t1.fuente = t2.fuente)
left join aux4 t3 on (t1.new_id = t3.new_id)
where categoria_2_f not in ("Descartable")
;

compute stats proceso.Dataton22_tabla_fiabilidad_reciente
;




--------------------------------------------
-------- IDEAS INSPIRACIONALES -------------
--------------------------------------------
--- 1. tener un mejor indicador de fiabilidad fuente de noticia 
----------------------------------------------
----------------------------------------------


----------------------------------------------
------- modelo recomendacion------------------
----------------------------------------------
drop table if exists proceso.Dataton22_tabla_recomendacion purge
;
create table  proceso.Dataton22_tabla_recomendacion stored as parquet as
with aux as(
select *,

(CASE 
when participacion = "Cliente" then 2
when participacion = "Sector" then 1
end) as peso_participacion,

(Case
when categoria = 'Alianzas' then 1.4
when categoria = 'Macroeconomia' then 2
when categoria = 'Reputacion' then 1.2
when categoria = 'Sostenibilidad' then 1.8
when categoria = 'Regulaciones' then 1.6
when categoria = 'Innovacion' then 1.7
when categoria = 'Otra' then 0.5
END) as peso_categoria,

(Case
when categoria = 'Alianzas' then rank_ind_alianzas
when categoria = 'Macroeconomia' then rank_ind_macroeconomico
when categoria = 'Reputacion' then rank_ind_reputacion
when categoria = 'Sostenibilidad' then rank_ind_sostenibilidad
when categoria = 'Regulaciones' then rank_ind_regulaciones
when categoria = 'Innovacion' then rank_ind_innovacion
when categoria = 'Otra' then 0
end) as peso_intra_categoria

from proceso.Dataton22_tabla_fiabilidad_reciente
),

aux2 as(
select *,
(peso_participacion * peso_categoria) + (rank_fiabilidad_fuente + ind_reciente) as score1
from aux
)

select *,
row_number() over(partition by nit order by score1 desc, peso_intra_categoria desc) as recomendacion
from aux2
;

compute stats proceso.Dataton22_tabla_recomendacion
;



select 
nit,
nombre,
new_id,
fuente,
news_title,
participacion,
categoria,
--rank_fiabilidad_fuente,
--ind_reciente,
--peso_participacion,
--peso_categoria,
score1,
peso_intra_categoria,
recomendacion

from proceso.Dataton22_tabla_recomendacion
limit 300
;
----------------------------------------------
------- Entregable 2--------------------------
----------------------------------------------

drop table if exists Dataton22_entregable_2 purge
;
--explain
create table  Dataton22_entregable_2 stored as parquet as

select 
distinct
'dataton_owners' as nombre_equipo,
nit,
new_id,
participacion,
categoria,
recomendacion
from proceso.Dataton22_tabla_recomendacion
;

compute stats Dataton22_entregable_2
;

----------------------------------------------

--------------------------------------------
-------- IDEAS INSPIRACIONALES -------------
--------------------------------------------
--- 1. el sisitema de score no es fiable, realizar una mejor version con modelos de ML
----------------------------------------------
----------------------------------------------


