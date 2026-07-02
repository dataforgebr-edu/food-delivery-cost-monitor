with source as (
    select * from {{ref("int_cost_by_domain")}}
),
calculate_window_functions_metrics as (
    select
        dt_execucao
        ,nm_dominio
        ,nr_total_jobs
        ,nr_sucesso_jobs
        ,vl_total_custo_usd
        ,vl_medio_por_job
        ,vl_maior_custo
        ,nr_total_duracao_minutos
        ,nr_medio_duracao_minutos
        ,nr_taxa_sucesso
        ,avg(vl_total_custo_usd) over (
            partition by nm_dominio
            order by dt_execucao asc
            rows between 6 preceding and current row
        ) as vl_medio_7d
        ,STDDEV_POP(vl_total_custo_usd) over (
            partition by nm_dominio
            order by dt_execucao asc
            rows between 6 preceding and current row
        ) as vl_desvio_7d
        ,sum(vl_total_custo_usd) over (
            partition by nm_dominio, date_trunc('month', dt_execucao)
            order by dt_execucao asc
        ) as vl_custo_mtd
        ,coalesce(
            lag(vl_total_custo_usd, 1) over (
                partition by nm_dominio
                order by dt_execucao asc
            ), vl_total_custo_usd) as vl_custo_dia_anterior
    from source
),
calculate_cost_and_trend_score as (
    select
        dt_execucao
        ,nm_dominio
        ,nr_total_jobs
        ,nr_sucesso_jobs
        ,vl_total_custo_usd
        ,vl_medio_por_job
        ,vl_maior_custo
        ,nr_total_duracao_minutos
        ,nr_medio_duracao_minutos
        ,nr_taxa_sucesso
        ,vl_medio_7d
        ,vl_desvio_7d
        ,vl_custo_mtd
        ,vl_custo_dia_anterior
        ,(vl_total_custo_usd - vl_custo_dia_anterior) / vl_custo_dia_anterior as nr_custo_trend_pct
        ,coalesce(
            least(vl_medio_7d / vl_total_custo_usd, 1.0),
            1.0) as nr_custo_score
        ,coalesce(
            least(vl_custo_dia_anterior / vl_total_custo_usd, 1.0),
            1.0) as nr_custo_trend_score
    from calculate_window_functions_metrics
),
calculate_efficiency_score as (
    select
        dt_execucao
        ,nm_dominio
        ,nr_total_jobs
        ,nr_sucesso_jobs
        ,vl_total_custo_usd
        ,vl_medio_por_job
        ,vl_maior_custo
        ,nr_total_duracao_minutos
        ,nr_medio_duracao_minutos
        ,nr_taxa_sucesso
        ,vl_medio_7d
        ,vl_desvio_7d
        ,vl_custo_mtd
        ,vl_custo_dia_anterior
        ,nr_custo_trend_pct
        ,nr_custo_score
        ,nr_custo_trend_score
        ,(nr_taxa_sucesso * 40) + (nr_custo_score * 40) + (nr_custo_trend_score * 20) as nr_eficiencia_score
    from calculate_cost_and_trend_score
)
select * from calculate_efficiency_score
