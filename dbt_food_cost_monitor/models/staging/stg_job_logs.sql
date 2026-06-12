with source as (
    select * from {{source('raw', 'job_logs')}}
),

transform as (
    select
        cast(job_id as {{dbt.type_string()}}) as id_job,
        cast(execution_date as date) as dt_execucao,
        cast(domain as {{dbt.type_string()}}) as nm_dominio,
        cast(job_name as {{dbt.type_string()}})  as nm_job,
        cast(duration_min as {{dbt.type_float()}})as nr_duracao_minutos,
        cast(dbu_consumed as {{dbt.type_float()}}) as nr_dbu_consumidos,
        cast(estimated_cost_usd as {{dbt.type_float()}}) as nr_estimativa_custo_usd,
        cast(status as {{dbt.type_string()}}) as st_status,
        cast(cluster_type as {{dbt.type_string()}}) as tp_cluster,
        cast(created_at as {{dbt.type_timestamp()}}) as dt_criacao,
        week_of_year(cast(execution_date as date)) as nr_semana_ano
    from source
)

select * from transform
