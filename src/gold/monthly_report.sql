WITH 
-- Lucro Total e Lucro Líquido por Mês
cte_lucro_liquido AS (
    SELECT 
        DATE_TRUNC('month', dataEmissao) AS mesRef,
        ROUND(SUM(totalRecebido), 2) AS lucroTotal,
        ROUND(SUM(totalRecebido - (custoCombustivelIda + custoCombustivelVolta)), 2) AS lucroLiquido
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE_TRUNC('month', dataEmissao)
),

-- Receita Média por Mês
cte_receita_media AS (
    SELECT 
        DATE_TRUNC('month', dataEmissao) AS mesRef,
        ROUND(AVG(totalRecebido), 2) AS receitaMedia
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE_TRUNC('month', dataEmissao)
),

-- Total de Entregas (apenas ida) por Mês
cte_total_entregas AS (
    SELECT 
        DATE_TRUNC('month', dataEmissao) AS mesRef,
        COUNT(*) AS totalEntregas
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE_TRUNC('month', dataEmissao)
),

-- Distância Total Percorrida por Mês
cte_distancia_total AS (
    SELECT 
        DATE_TRUNC('month', dataEmissao) AS mesRef,
        ROUND(SUM(distanciaIdaKm + distanciaVoltaKm), 2) AS distanciaTotalKm
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE_TRUNC('month', dataEmissao)
),

-- Soma Total de Gasto com Combustível por Mês
cte_gasto_combustivel AS (
    SELECT 
        DATE_TRUNC('month', dataEmissao) AS mesRef,
        ROUND(SUM(custoCombustivelIda + custoCombustivelVolta), 2) AS totalGastoCombustivel
    FROM 
        silver.upsell.viagens
    GROUP BY 
        DATE_TRUNC('month', dataEmissao)
)

-- Resultado Consolidado
SELECT 
    ll.mesRef,
    ll.lucroTotal,
    ll.lucroLiquido,
    rm.receitaMedia,
    te.totalEntregas,
    dt.distanciaTotalKm,
    gc.totalGastoCombustivel
FROM 
    cte_lucro_liquido ll
LEFT JOIN cte_receita_media rm ON ll.mesRef = rm.mesRef
LEFT JOIN cte_total_entregas te ON ll.mesRef = te.mesRef
LEFT JOIN cte_distancia_total dt ON ll.mesRef = dt.mesRef
LEFT JOIN cte_gasto_combustivel gc ON ll.mesRef = gc.mesRef
ORDER BY 
    ll.mesRef