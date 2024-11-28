SELECT
    ID_viagem AS idViagem,
    TOT_RECEB AS totalRecebido,
    CUSTO_COMBUSTIVEL_IDA AS custoCombustivelIda,
    CUSTO_COMBUSTIVEL_VOLTA AS custoCombustivelVolta,
    TOT_RECEB - (CUSTO_COMBUSTIVEL_IDA + CUSTO_COMBUSTIVEL_VOLTA) AS lucroLiquido,
    CLIENTE_ORIGEM AS clienteOrigem,
    CLIENTE_COLETA AS clienteColeta
FROM bronze.upsell.viagens