%sql
SELECT
    ID_viagem AS idViagem,
    DES AS destino,
    EMISSAO AS dataEmissao,
    CLIENTE_ORIGEM AS clienteOrigem,
    ORIGEM_CIDADE AS origemCidade,
    ORIGEM_ESTADO AS origemEstado,
    CLIENTE_COLETA AS clienteColeta,
    COLETA_CIDADE AS coletaCidade,
    COLETA_ESTADO AS coletaEstado
FROM bronze.upsell.viagens