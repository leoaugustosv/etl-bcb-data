import requests
import json

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
        "nome": "Leasing de veículos",
        "codigoSegmento": "1",
        "codigoModalidade": "120a101",
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


for produto in PRODUTOS:

    segmento = produto["codigoSegmento"]
    modalidade = produto["codigoModalidade"]

    base = f"https://www.bcb.gov.br/api/servico/sitebcb/historicotaxajurosdiario/TodosCampos?filtro="
    filtro = f"(codigoSegmento eq '{segmento}') and (codigoModalidade eq '{modalidade}') and (InicioPeriodo ge '2025-03-24')"

    url = f"{base}{filtro}"

    body = requests.get(url=url).text

    json_body = json.loads(body)["conteudo"]

    print(json_body)