from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, TimestampType

SCHEMA = StructType([
    StructField("NM_BANK", StringType(), True),
    StructField("NM_SEGM", StringType(), True),
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

SCHEMA_AUXILIAR = StructType([
    StructField("VL_TAXA", DoubleType(), True),
    StructField("dat_ref_carga", StringType(), True),
])

NOME_MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

URL_PARAMETROS = "https://www.bcb.gov.br/api/servico/sitebcb/HistoricoTaxaJurosDiario/ParametrosConsulta"
URL_PERIODOS = "https://www.bcb.gov.br/api/servico/sitebcb/HistoricoTaxaJurosDiario/ConsultaDatas"
URL_SELIC = "https://www.bcb.gov.br/api/servico/sitebcb/bcdatasgs?tronco=estatisticas&dataInicial=data_inicio_selic&dataFinal=dd/mm/yyyy&serie=432"
URL_ICC_CUSTO_CRED = "https://www.bcb.gov.br/api/servico/sitebcb/bcdatasgs?tronco=estatisticas&dataInicial=01/01/2012&dataFinal=dd/mm/yyyy&serie=25351"
URL_INFLACAO = "https://www.bcb.gov.br/api/servico/sitebcb/bcdatasgs?tronco=estatisticas&dataInicial=01/01/2012&dataFinal=dd/mm/yyyy&serie=13522"
URL_TAXA_DESEMPREGO = "https://www.bcb.gov.br/api/servico/sitebcb/bcdatasgs?tronco=estatisticas&dataInicial=01/01/2012&dataFinal=dd/mm/yyyy&serie=24369"

INSTITUICOES_ESPECIFICAS = {
    "BCO SANTANDER (BRASIL) S.A.": "red",
    "ITAÚ UNIBANCO HOLDING S.A.": "coral",
    "FIN. ITAU CBD CFI": "darkorange",
    "ITAÚ UNIBANCO S.A.": "orange",
    "BANCO ITAÚ CONSIGNADO S.A.": "orangered",
    "BCO BRADESCO S.A.": "magenta",
    "BCO BRADESCO FINANC. S.A.": "dimgray",
    "BANCO BRADESCARD": "violet",
    "NU FINANCEIRA S.A. CFI": "indigo",
    "CAIXA ECONOMICA FEDERAL": "blue",
    "BCO DO BRASIL S.A.": "gold",
    "BCO. J.SAFRA S.A.": "khaki",
    "BCO SAFRA S.A.": "darkslategray",
    "SAFRA CFI S.A.": "teal",
    "BANCO BTG PACTUAL S.A.": "midnightblue",
    "BCO COOPERATIVO SICREDI S.A.": "green",
    "BANCO INTER":"brown",
}