%sql
SELECT
    ID_viagem AS idViagem,
    DES AS destino,
    EMISSAO AS dataEmissao,
    VEICULO AS veiculo,
    MOTORISTA AS motorista,
    TOT_RECEB AS totalRecebido,
    CLIENTE_ORIGEM AS clienteOrigem,
    ORIGEM_CIDADE AS origemCidade,
    ORIGEM_ESTADO AS origemEstado,
    CLIENTE_COLETA AS clienteColeta,
    COLETA_CIDADE AS coletaCidade,
    COLETA_ESTADO AS coletaEstado,
    DISTANCIA_IDA_KM AS distanciaIdaKm,
    DISTANCIA_VOLTA_KM AS distanciaVoltaKm,
    TEMPO_ESTIM_IDA_MIN AS tempoEstimadoIda,
    TEMPO_ESTIM_VOLTA_MIN AS tempoEstimadoVolta,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    CAPACIDADE_CARGA_TONELADAS AS capacidadeCargaToneladas,
    UTILIZACAO_VEICULO AS utilizacaoVeiculo,
    TIPO_RODOVIA AS tipoRodovia
FROM bronze.upsell.viagens