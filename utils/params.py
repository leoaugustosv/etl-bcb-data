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


PRODUTOS_TST = [
    {
        "nome": "Capital de giro com prazo superior a 365 dias",
        "codigoSegmento": "2",
        "codigoModalidade": "211101",
    },
]


PRODUTOS = [
    {
        "nome": "Aquisição de outros bens",
        "codigoSegmento": "1",
        "codigoModalidade": "402101",
    },
    {
        "nome": "Aquisição de veículos",
        "codigoSegmento": "1",
        "codigoModalidade": "401101",
    },
    {
        "nome": "Cartão de crédito parcelado",
        "codigoSegmento": "1",
        "codigoModalidade": "215101",
    },
    {
        "nome": "Cartão de crédito rotativo",
        "codigoSegmento": "1",
        "codigoModalidade": "204101",
    },
    {
        "nome": "Cheque especial",
        "codigoSegmento": "1",
        "codigoModalidade": "216101",
    },
    {
        "nome": "Crédito pessoal consignado INSS",
        "codigoSegmento": "1",
        "codigoModalidade": "218101",
    },
    {
        "nome": "Crédito pessoal consignado privado",
        "codigoSegmento": "1",
        "codigoModalidade": "219101",
    },
    {
        "nome": "Crédito pessoal consignado público",
        "codigoSegmento": "1",
        "codigoModalidade": "220101",
    },
    {
        "nome": "Crédito pessoal não consignado",
        "codigoSegmento": "1",
        "codigoModalidade": "221101",
    },
    {
        "nome": "Desconto de cheques",
        "codigoSegmento": "1",
        "codigoModalidade": "302101",
    },
    {
        "nome": "Financiamento imobiliário com taxas de mercado",
        "codigoSegmento": "1",
        "codigoModalidade": "903101",
    },
    {
        "nome": "Financiamento imobiliário com taxas reguladas",
        "codigoSegmento": "1",
        "codigoModalidade": "905101",
    },
    {
        "nome": "Arrendamento mercantil de veículos",
        "codigoSegmento": "1",
        "codigoModalidade": "1205101",
    },
    {
        "nome": "Antecipação de faturas de cartão de crédito",
        "codigoSegmento": "2",
        "codigoModalidade": "303101",
    },
    {
        "nome": "Capital de giro com prazo até 365 dias",
        "codigoSegmento": "2",
        "codigoModalidade": "210101",
    },
    {
        "nome": "Capital de giro com prazo superior a 365 dias",
        "codigoSegmento": "2",
        "codigoModalidade": "211101",
    },
    {
        "nome": "Cheque especial",
        "codigoSegmento": "2",
        "codigoModalidade": "216101",
    },
    {
        "nome": "Conta garantida",
        "codigoSegmento": "2",
        "codigoModalidade": "217101",
    },
    {
        "nome": "Desconto de cheques",
        "codigoSegmento": "2",
        "codigoModalidade": "302101",
    },
    {
        "nome": "Desconto de duplicata",
        "codigoSegmento": "2",
        "codigoModalidade": "301101",
    },
    {
        "nome": "Vendor",
        "codigoSegmento": "2",
        "codigoModalidade": "404101",
    },
    {
        "nome": "Capital de giro com prazo até 365 dias",
        "codigoSegmento": "2",
        "codigoModalidade": "210204",
    },
    {
        "nome": "Capital de giro com prazo superior a 365 dias",
        "codigoSegmento": "2",
        "codigoModalidade": "211204",
    },
    {
        "nome": "Conta garantida",
        "codigoSegmento": "2",
        "codigoModalidade": "217204",
    },
    {
        "nome": "Adiantamento sobre contratos de câmbio",
        "codigoSegmento": "2",
        "codigoModalidade": "502205",
    },
]
