with source as (
    select * from {{ref('stg_job_logs')}}
),
agegate_data_by_domain as (
    select
        dt_execucao,
        nm_dominio,
        count(id_job) as nr_total_jobs,
        count(
            case
                when st_status = 'success' then id_job
            end
         ) as nr_sucesso_jobs,
        sum(nr_estimativa_custo_usd) as vl_total_custo_usd,
        avg(nr_estimativa_custo_usd) as vl_medio_por_job,
        max(nr_estimativa_custo_usd) as vl_maior_custo,
        sum(nr_duracao_minutos) as nr_total_duracao_minutos,
        avg(nr_duracao_minutos) as nr_medio_duracao_minutos
    from source
    group by
        dt_execucao,
        nm_dominio
),
calculation_success_rate as (
    select
        dt_execucao,
        nm_dominio,
        nr_total_jobs
        nr_sucesso_jobs,
        vl_total_custo_usd,
        vl_medio_por_job,
        vl_maior_custo,
        nr_total_duracao_minutos,
        nr_medio_duracao_minutos,
        cast(nr_sucesso_jobs as {{dbt.type_float()}}) / nr_total_jobs as nr_taxa_sucesso
    from agegate_data_by_domain
)

select * from calculation_success_rate
