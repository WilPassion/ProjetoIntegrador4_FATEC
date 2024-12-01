WITH 
-- Lucro Total e Lucro Líquido por Dia
cte_lucro_liquido AS (
    SELECT 
        DATE(dataEmissao) AS dtRef,
        ROUND(SUM(totalRecebido), 2) AS lucroTotal,
        ROUND(SUM(totalRecebido - (custoCombustivelIda + custoCombustivelVolta)), 2) AS lucroLiquido
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE(dataEmissao)
),

-- Receita Média por Dia
cte_receita_media AS (
    SELECT 
        DATE(dataEmissao) AS dtRef,
        ROUND(AVG(totalRecebido), 2) AS receitaMedia
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE(dataEmissao)
),

-- Total de Entregas (apenas ida)
cte_total_entregas AS (
    SELECT 
        DATE(dataEmissao) AS dtRef,
        COUNT(*) AS totalEntregas
    FROM 
        silver.upsell.viagens

    GROUP BY 
        DATE(dataEmissao)
),

-- Distância Total Percorrida por Dia
cte_distancia_total AS (
    SELECT 
        DATE(dataEmissao) AS dtRef,
        ROUND(SUM(distanciaIdaKm + distanciaVoltaKm), 2) AS distanciaTotalKm
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE(dataEmissao)
),

-- Soma Total de Gasto com Combustível
cte_gasto_combustivel AS (
    SELECT 
        DATE(dataEmissao) AS dtRef,
        ROUND(SUM(custoCombustivelIda + custoCombustivelVolta), 2) AS totalGastoCombustivel
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE(dataEmissao)
)

-- Resultado Consolidado
SELECT 
    ll.dtRef,
    ll.lucroTotal,
    ll.lucroLiquido,
    rm.receitaMedia,
    te.totalEntregas,
    dt.distanciaTotalKm,
    gc.totalGastoCombustivel
FROM 
    cte_lucro_liquido ll
LEFT JOIN cte_receita_media rm ON ll.dtRef = rm.dtRef
LEFT JOIN cte_total_entregas te ON ll.dtRef = te.dtRef
LEFT JOIN cte_distancia_total dt ON ll.dtRef = dt.dtRef
LEFT JOIN cte_gasto_combustivel gc ON ll.dtRef = gc.dtRef
ORDER BY 
    ll.dtRef