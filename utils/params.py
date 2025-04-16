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

NOME_MESES = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

URL_PARAMETROS = "https://www.bcb.gov.br/api/servico/sitebcb/HistoricoTaxaJurosDiario/ParametrosConsulta"
URL_PERIODOS = "https://www.bcb.gov.br/api/servico/sitebcb/HistoricoTaxaJurosDiario/ConsultaDatas"

PRODUTOS_BKP = [
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Aquisição de outros bens - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "402101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Aquisição de veículos - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "401101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Arrendamento mercantil de veículos - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "1205101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Cartão de crédito - parcelado - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "215101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Cartão de crédito - rotativo total - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "204101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Cheque especial - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "216101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Crédito pessoal consignado INSS - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "218101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Crédito pessoal consignado privado - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "219101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Crédito pessoal consignado público - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "220101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Crédito pessoal não-consignado - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "221101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Desconto de cheques - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "302101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Financiamento imobiliário com taxas de mercado - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "903101", 
        "tipoModalidade": "M"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Financiamento imobiliário com taxas de mercado - Pós-fixado referenciado em IPCA", 
        "codigoSegmento": "1", 
        "codigoModalidade": "903203", 
        "tipoModalidade": "M"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Financiamento imobiliário com taxas de mercado - Pós-fixado referenciado em TR", 
        "codigoSegmento": "1", 
        "codigoModalidade": "903201", 
        "tipoModalidade": "M"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Financiamento imobiliário com taxas reguladas - Pré-fixado", 
        "codigoSegmento": "1", 
        "codigoModalidade": "905101", 
        "tipoModalidade": "M"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Financiamento imobiliário com taxas reguladas - Pós-fixado referenciado em IPCA", 
        "codigoSegmento": "1", 
        "codigoModalidade": "905203", 
        "tipoModalidade": "M"
    },
    {
        "Segmento": "Pessoa Física", 
        "Modalidade": "Financiamento imobiliário com taxas reguladas - Pós-fixado referenciado em TR", 
        "codigoSegmento": "1", 
        "codigoModalidade": "905201", 
        "tipoModalidade": "M"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Adiantamento sobre contratos de câmbio (ACC) - Pós-fixado referenciado em moeda estrangeira", 
        "codigoSegmento": "2", 
        "codigoModalidade": "502205", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Antecipação de faturas de cartão de crédito - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "303101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Capital de giro com prazo até 365 dias - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "210101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Capital de giro com prazo até 365 dias - Pós-fixado referenciado em juros flutuantes", 
        "codigoSegmento": "2", 
        "codigoModalidade": "210204", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Capital de giro com prazo superior a 365 dias - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "211101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Capital de giro com prazo superior a 365 dias - Pós-fixado referenciado em juros flutuantes", 
        "codigoSegmento": "2", 
        "codigoModalidade": "211204", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Cheque especial - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "216101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Conta garantida - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "217101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Conta garantida - Pós-fixado referenciado em juros flutuantes", 
        "codigoSegmento": "2", 
        "codigoModalidade": "217204", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Desconto de cheques - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "302101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Desconto de duplicatas - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "301101", 
        "tipoModalidade": "D"
    },
    {
        "Segmento": "Pessoa Jurídica", 
        "Modalidade": "Vendor - Pré-fixado", 
        "codigoSegmento": "2", 
        "codigoModalidade": "404101", 
        "tipoModalidade": "D"
    }
]