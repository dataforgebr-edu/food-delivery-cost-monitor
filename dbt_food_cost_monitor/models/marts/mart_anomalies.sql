with source as (
    select * from {{ref('stg_job_logs')}}
),
calculate_job_stats as (
    select
        id_job
        ,dt_execucao
        ,nm_dominio
        ,nm_job
        ,vl_estimativa_custo_usd
        ,avg(vl_estimativa_custo_usd) over (
            partition by nm_dominio, nm_job
            order by dt_execucao asc
            rows between 6 preceding and current row
        ) as vl_medio_7d
        ,STDDEV_POP(vl_estimativa_custo_usd) over (
            partition by nm_dominio, nm_job
            order by dt_execucao asc
            rows between 6 preceding and current row
        ) as vl_desvio_7d
    from source
),
calculate_sigma as (
    select
        id_job
        ,dt_execucao
        ,nm_dominio
        ,nm_job
        ,vl_estimativa_custo_usd
        ,vl_medio_7d
        ,vl_desvio_7d
        ,vl_medio_7d + (2 * vl_desvio_7d) as vl_limite_superior
        ,(vl_estimativa_custo_usd - vl_medio_7d) / NULLIF(vl_desvio_7d, 0) as nr_sigma_desvio
    from calculate_job_stats
),
detect_anomalies as (
    select
        id_job
        ,dt_execucao
        ,nm_dominio
        ,nm_job
        ,vl_estimativa_custo_usd
        ,vl_medio_7d
        ,vl_desvio_7d
        ,vl_limite_superior
        ,nr_sigma_desvio
        ,case
            when nr_sigma_desvio > 3 then 'critical'
            when nr_sigma_desvio >= 2 then 'warning'
        end as nm_severidade
    from calculate_sigma
    where 1 = 1
    and vl_estimativa_custo_usd > vl_limite_superior
)
select * from detect_anomalies
