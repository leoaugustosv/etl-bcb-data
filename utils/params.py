from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, TimestampType

SCHEMA = StructType([
    StructField("NM_BANK", StringType(), True),
    StructField("NM_SEGM", StringType(), True),
    StructField("DS_PROD", StringType(), True),
    StructField("CD_SEGM", StringType(), True),
    StructField("CD_MODL", StringType(), True),
    StructField("NR_POSI", IntegerType(), True),
    StructField("DS_MODL", StringType(), True),
    StructField("IN_INIC_PERI_EXAT", IntegerType(), True),
    StructField("DT_APROX", StringType(), True),
    StructField("QT_DIA_APROX", IntegerType(), True),
    StructField("VL_TAXA_JURO_AM", DoubleType(), True),
    StructField("VL_TAXA_JURO_AA", DoubleType(), True),
    StructField("dat_ref_carga", StringType(), True),
    StructField("dh_exec", TimestampType(), True),
])

NOME_MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

URL_PARAMETROS = "https://www.bcb.gov.br/api/servico/sitebcb/HistoricoTaxaJurosDiario/ParametrosConsulta"

PRODUTOS_BKP = [
    {
        "nome": "Adiantamento sobre contratos de câmbio (ACC) - Pós-fixado referenciado em moeda estrangeira",
        "codigoSegmento": "2",
        "codigoModalidade": "502205"
    },
    {
        "nome": "Antecipação de faturas de cartão de crédito - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "303101"
    },
    {
        "nome": "Capital de giro com prazo até 365 dias - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "210101"
    },
    {
        "nome": "Capital de giro com prazo até 365 dias - Pós-fixado referenciado em juros flutuantes",
        "codigoSegmento": "2",
        "codigoModalidade": "210204"
    },
    {
        "nome": "Capital de giro com prazo superior a 365 dias - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "211101"
    },
    {
        "nome": "Capital de giro com prazo superior a 365 dias - Pós-fixado referenciado em juros flutuantes",
        "codigoSegmento": "2",
        "codigoModalidade": "211204"
    },
    {
        "nome": "Cheque especial - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "216101"
    },
    {
        "nome": "Conta garantida - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "217101"
    },
    {
        "nome": "Conta garantida - Pós-fixado referenciado em juros flutuantes",
        "codigoSegmento": "2",
        "codigoModalidade": "217204"
    },
    {
        "nome": "Desconto de cheques - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "302101"
    },
    {
        "nome": "Desconto de duplicatas - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "301101"
    },
    {
        "nome": "Vendor - Pré-fixado",
        "codigoSegmento": "2",
        "codigoModalidade": "404101"
    }
]
