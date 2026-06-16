with source as (
    select * from {{source('raw', 'job_logs')}}
),
change_cast_types as (
    select
        cast(job_id as {{dbt.type_string()}}) as id_job,
        cast(execution_date as date) as dt_execucao,
        cast(domain as {{dbt.type_string()}}) as nm_dominio,
        cast(job_name as {{dbt.type_string()}})  as nm_job,
        cast(duration_min as {{dbt.type_float()}})as nr_duracao_minutos,
        cast(dbu_consumed as {{dbt.type_float()}}) as nr_dbu_consumidos,
        cast(estimated_cost_usd as {{dbt.type_float()}}) as vl_estimativa_custo_usd,
        cast(status as {{dbt.type_string()}}) as st_status,
        cast(cluster_type as {{dbt.type_string()}}) as tp_cluster,
        cast(created_at as {{dbt.type_timestamp()}}) as dt_criacao
    from source
),
filter_negative_cost as (
    select
        id_job,
        dt_execucao,
        nm_dominio,
        nm_job,
        nr_duracao_minutos,
        nr_dbu_consumidos,
        vl_estimativa_custo_usd,
        st_status,
        tp_cluster,
        dt_criacao
    from change_cast_types
    where 1 = 1
    and vl_estimativa_custo_usd > 0
),
create_new_columns as (
    select
        id_job,
        dt_execucao,
        nm_dominio,
        nm_job,
        nr_duracao_minutos,
        nr_dbu_consumidos,
        vl_estimativa_custo_usd,
        st_status,
        tp_cluster,
        dt_criacao,
        cast(week(dt_execucao) as int) as nr_semana
    from filter_negative_cost
)

select * from create_new_columns
