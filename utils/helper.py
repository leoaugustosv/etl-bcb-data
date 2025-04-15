import requests
import json
import calendar
from datetime import date, datetime, timedelta
from utils.params import NOME_MESES


def obter_produtos_bacen(params_url):
    params_list = []
    try:
        body = requests.get(url=params_url).text
        json_body = json.loads(body)["conteudo"]

    except Exception as e:
        json_body = []
        print(f"ERRO: Falha ao obter parâmetros usando a URL: {params_url} - {e}")
    
    if json_body:
        print(f"PARÂMETROS: {len(json_body)} produtos encontrados.")
        for produto in json_body:
            product_dict = {}
            product_dict["nome"] = produto["Modalidade"]
            product_dict["codigoSegmento"] = produto["codigoSegmento"]
            product_dict["codigoModalidade"] = produto["codigoModalidade"]
            params_list.append(product_dict)
    else:
        print(f"PARÂMETROS: Nenhum produto foi encontrado.")
        
    return params_list




def enviar_request_bacen(url):
    try:
        body = requests.get(url=url).text
        json_body = json.loads(body)["conteudo"]

    except Exception as e:
        print(f"ERRO: Falha ao enviar request para URL: {url} - {e}")
        json_body = []
    return json_body


def obter_datas_do_mes(mes_string:str):

    ano, mes = map(int, mes_string.split("-"))

    numero_dias = calendar.monthrange(ano, mes)[1]

    lista_datas = [
        f"{ano:04d}-{mes:02d}-{dia:02d}" for dia in range(1, numero_dias+1)
    ]

    return lista_datas


def obter_datas_do_ano(ano_str:str, mes_inicial:int, mes_final:int):

    ano = int(ano_str)
    lista_datas_ano = []

    for mes in range(mes_inicial, mes_final+1):
        
        numero_dias = calendar.monthrange(int(ano), mes)[1]
    
        lista_datas_mes = [
            f"{ano:04d}-{mes:02d}-{dia:02d}" for dia in range(1, numero_dias+1)
        ]

        for data in lista_datas_mes:
            lista_datas_ano.append(data)

    return lista_datas_ano


def data_com_dias_subtraidos(data:str, dias:int):
    data_convertida = datetime.strptime(data, '%Y-%m-%d') - timedelta(days=dias)
    return data_convertida.strftime('%Y-%m-%d')


def diferenca_dias_datas(data_maior:str, data_menor:str):
    data1 = datetime.strptime(data_maior, '%Y-%m-%d')
    data2 = datetime.strptime(data_menor, '%Y-%m-%d')
    return (data1 - data2).days

def transform_passo_1(json_body, df_list, data_alvo, nome, segmento, modalidade):
    for item in json_body:
        df_dict = {}

        df_dict["NM_BANK"] = item["InstituicaoFinanceira"]
        df_dict["NM_SEGM"] = item["Segmento"]
        df_dict["DS_PROD"] = nome
        df_dict["CD_SEGM"] = segmento
        df_dict["CD_MODL"] = modalidade
        df_dict["NR_POSI"] = item["Posicao"]
        df_dict["DS_MODL"] = item["Modalidade"]
        df_dict["IN_INIC_PERI_EXAT"] = 1
        df_dict["DT_APROX"] = None
        df_dict["QT_DIA_APROX"] = None
        df_dict["VL_TAXA_JURO_AM"] = float(item["TaxaJurosAoMes"].replace(",","."))
        df_dict["VL_TAXA_JURO_AA"] = float(item["TaxaJurosAoAno"].replace(",","."))
        df_dict["dat_ref_carga"] = data_alvo
        df_dict["dh_exec"] = datetime.now()

        df_list.append(df_dict)

    print(f"OK: Dados do produto {nome.upper()} extraídos! - {data_alvo}")

    return df_list



def transform_passo_2(json_body, df_list, data_alvo, nome, segmento, modalidade):
    for item in json_body:
        df_dict = {}

        df_dict["NM_BANK"] = item["InstituicaoFinanceira"]
        df_dict["NM_SEGM"] = item["Segmento"]
        df_dict["DS_PROD"] = nome
        df_dict["CD_SEGM"] = segmento
        df_dict["CD_MODL"] = modalidade
        df_dict["NR_POSI"] = item["Posicao"]
        df_dict["DS_MODL"] = item["Modalidade"]
        df_dict["IN_INIC_PERI_EXAT"] = 0
        df_dict["DT_APROX"] = data_alvo
        df_dict["QT_DIA_APROX"] = None
        df_dict["VL_TAXA_JURO_AM"] = float(item["TaxaJurosAoMes"].replace(",","."))
        df_dict["VL_TAXA_JURO_AA"] = float(item["TaxaJurosAoAno"].replace(",","."))
        df_dict["dat_ref_carga"] = data_alvo
        df_dict["dh_exec"] = datetime.now()

        df_list.append(df_dict)

    print(f"OK: Dados do produto {nome.upper()} extraídos! - {data_alvo}")

    return df_list



def transform_passo_3(json_body, df_list, data_alvo, nome, segmento, modalidade, data_aprox):
    for item in json_body:
        df_dict = {}

        df_dict["NM_BANK"] = item["InstituicaoFinanceira"]
        df_dict["NM_SEGM"] = item["Segmento"]
        df_dict["DS_PROD"] = nome
        df_dict["CD_SEGM"] = segmento
        df_dict["CD_MODL"] = modalidade
        df_dict["NR_POSI"] = item["Posicao"]
        df_dict["DS_MODL"] = item["Modalidade"]
        df_dict["IN_INIC_PERI_EXAT"] = 0
        df_dict["DT_APROX"] = data_aprox
        df_dict["QT_DIA_APROX"] = diferenca_dias_datas(data_alvo, data_aprox)
        df_dict["VL_TAXA_JURO_AM"] = float(item["TaxaJurosAoMes"].replace(",","."))
        df_dict["VL_TAXA_JURO_AA"] = float(item["TaxaJurosAoAno"].replace(",","."))
        df_dict["dat_ref_carga"] = data_alvo
        df_dict["dh_exec"] = datetime.now()

        df_list.append(df_dict)

    print(f"OK: Dados do produto {nome.upper()} extraídos! - {data_aprox}")

    return df_list



def transform_null(json_body, df_list, data_alvo, nome, segmento, modalidade):
    for item in json_body:
        df_dict = {}

        df_dict["NM_BANK"] = None
        df_dict["NM_SEGM"] = None
        df_dict["DS_PROD"] = nome
        df_dict["CD_SEGM"] = segmento
        df_dict["CD_MODL"] = modalidade
        df_dict["NR_POSI"] = None
        df_dict["DS_MODL"] = None
        df_dict["IN_INIC_PERI_EXAT"] = None
        df_dict["DT_APROX"] = None
        df_dict["QT_DIA_APROX"] = None
        df_dict["VL_TAXA_JURO_AM"] = None
        df_dict["VL_TAXA_JURO_AA"] = None
        df_dict["dat_ref_carga"] = data_alvo
        df_dict["dh_exec"] = datetime.now()

        df_list.append(df_dict)

    return df_list







def extrair_dados_bacen_data_unica(PRODUTOS:list, data_alvo:str = None):

    df_list = []

    if not data_alvo:
        data_alvo = {date.today()}

    for produto in PRODUTOS:
        
        nome = produto["nome"]
        segmento = produto["codigoSegmento"]
        modalidade = produto["codigoModalidade"]
        print(f"\nSTART: Extraindo dados do produto --- {nome.upper()}...")

        
        base = f"https://www.bcb.gov.br/api/servico/sitebcb/historicotaxajurosdiario/TodosCampos?filtro="
        filtro = f"(codigoSegmento eq '{segmento}') and (codigoModalidade eq '{modalidade}') and (InicioPeriodo eq '{data_alvo}')"
        filtro2 = f"(codigoSegmento eq '{segmento}') and (codigoModalidade eq '{modalidade}') and (FimPeriodo eq '{data_alvo}')"
        url = f"{base}{filtro}"
        json_body = enviar_request_bacen(url)

        
        if not json_body:
            # Passo 2
            url = f"{base}{filtro2}"
            json_body = enviar_request_bacen(url)

            if not json_body:
                # Passo 3
                days_limit = 10
                i = 0
                print(f"ATENÇÃO: Filtros de início e fim de período para o produto {nome.upper()} na data {data_alvo} não retornaram dados.")
                print(f"TENTATIVA: Buscando dados para o produto {nome.upper()} no período de início mais próximo...")
                while not json_body and i < days_limit:
                    i += 1
                    data_aprox = data_com_dias_subtraidos(data_alvo, i)
                    filtro3 = f"(codigoSegmento eq '{segmento}') and (codigoModalidade eq '{modalidade}') and (InicioPeriodo eq '{data_aprox}')"
                    url = f"{base}{filtro3}"
                    json_body = enviar_request_bacen(url)
                if i == days_limit:
                    print(f"ERRO: Não foi possível encontrar dados para o produto {nome.upper()} na data {data_alvo}. PULANDO PRODUTO.")
                    continue
                else:
                    df_list = transform_passo_3(json_body, df_list, data_alvo, nome, segmento, modalidade, data_aprox)
            
    
            else:
                df_list = transform_passo_2(json_body, df_list, data_alvo, nome, segmento, modalidade)
        
        else:
            # Passo 1
            df_list = transform_passo_1(json_body, df_list, data_alvo, nome, segmento, modalidade)

    return df_list





def extrair_dados_bacen_mes(PRODUTOS:list, mes:str = None):

    df_list = []

    if not mes:
        mes = date.today().strftime("%Y-%m")
    
    for data in obter_datas_do_mes(mes):
        print(f"\n----- {data} -----")
        for linha in extrair_dados_bacen_data_unica(PRODUTOS, data):
            df_list.append(linha)


    return df_list



def extrair_dados_bacen_ano(PRODUTOS:list, ano:str = None, mes_inicial:int = None, mes_final:int = None):

    df_list = []

    # Default jan-dez se range não for informado ou se inválido
    if not mes_inicial:
        mes_inicial = 1
    else:
        mes_inicial = 1 if mes_inicial < 1 or mes_inicial > 12 else mes_inicial

    if not mes_final:
        mes_final = 12
    else:
        mes_final = 12 if mes_final < 1 or mes_final > 12 else mes_final

    if not ano:
        ano = date.today().strftime("%Y")
        print(f"OBS: Ano não informado. Considerando ano atual ({ano})...")
    
    print(f"\nSTART: Extração iniciado para o ano {ano} - de {NOME_MESES[mes_inicial].upper()} até {NOME_MESES[mes_final].upper()}...")

    for data in obter_datas_do_ano(ano, mes_inicial, mes_final):
        print(f"\n----- {data} -----")
        for linha in extrair_dados_bacen_data_unica(PRODUTOS, data):
            df_list.append(linha)

    return df_list
