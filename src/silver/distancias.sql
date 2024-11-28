%sql
SELECT
    ID_viagem AS idViagem,
    DISTANCIA_IDA_KM AS distanciaIdaKm,
    DISTANCIA_VOLTA_KM AS distanciaVoltaKm,
    DISTANCIA_IDA_KM + DISTANCIA_VOLTA_KM AS distanciaTotalKm,
    TIPO_RODOVIA AS tipoRodovia
FROM bronze.upsell.viagens
