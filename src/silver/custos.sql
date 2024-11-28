%sql
SELECT
    ID_viagem AS idViagem,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    DISTANCIA_IDA_KM + DISTANCIA_VOLTA_KM AS distanciaTotalKm,
    (CUSTO_COMBUSTIVEL_IDA + CUSTO_COMBUSTIVEL_VOLTA) / 
    (DISTANCIA_IDA_KM + DISTANCIA_VOLTA_KM) AS custoPorKm
FROM bronze.upsell.viagens