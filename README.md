# ETL-BCB-DATA

Extraindo histórico de taxas de juros que cada banco ofertou em cada produto. 

**Fonte dos dados:** Banco Central do Brasil.

## Tabela gerada

- **Visão:** Instituição + Produto
- **Chave:** NM_BANK + CD_MODL

| Nome | Tipo | Descrição | Opções
| --- | --- | --- | --- |
| NM_BANK | string | Nome da instituição financeira | -
| NM_SEGM | string | Nome do segmento do produto | Pessoa Jurídica<br/>Pessoa Física
| DS_PROD | string | Descrição parametrizada do produto | -
| CD_SEGM | string | Código parametrizado do segmento do produto | 1 (PF)<br/>2 (PJ)
| CD_MODL | string | Código parametrizado da modalidade do produto | -
| NR_POSI | int | Posição do produto da instituição financeira em relação aos demais produtos. Ranqueado por taxa de juros em um período específico, em ordem crescente | -
| DS_MODL | string | Descrição da modalidade | -
| DT_ALVO | string | Data alvo em que se buscou a taxa de juros | -
| IN_INIC_PERI_EXAT | int | Indicador de taxa de juros encontrada na data alvo | 1 (SIM)<br/>2 (NÃO)
| DT_APROX | string | Data mais próxima da data alvo em que se encontrou a taxa de juros | -
| QT_DIA_APROX | int | Quantidade de dias em que a data foi aproximada | -
| VL_TAXA_JURO_AM | double | Valor da taxa de juros ao mês | -
| VL_TAXA_JURO_AA | double | Valor da taxa de juros ao ano | -
| dat_ref_carga | string | Data de referência da carga | -
| dh_exec | timestamp | Data de execução/ingestão | -


## Lógica
<details>
<summary><b>Lógica: <u>Taxa mais recente para a data alvo</u> (clique para expandir)</b></summary>

### PASSO 1
**Tentar filtrar InicioPeriodo = dataAlvo**.
- ✅ **SUCESSO**: marcar flag IN_INIC_PERI_EXAT com 1, DT_APROX nulo, QT_DIA_APROX nulo - ```(EXEMPLO: 24/03/2025)```

- ❌ **FALHA**: Próximo passo.


### PASSO 2 
**Se InicioPeriodo = dataAlvo não trouxer resultado, então filtrar FimPeriodo = dataAlvo**.
- ✅ **SUCESSO**: marcar flag IN_INIC_PERI_EXAT com 0, DT_APROX com o valor do FimPeriodo, QT_DIA_APROX nulo  - ```(EXEMPLO: 25/03/2025)```

- ❌ **FALHA**: Próximo passo.


### PASSO 3 
**Se FimPeriodo = dataAlvo não trouxer resultado, então filtrar InicioPeriodo mais próximo da dataAlvo (limite de até 10 dias atrás)**.
- ✅ **SUCESSO**: marcar flag IN_INIC_PERI_EXAT com 0, DT_APROX com o valor do InicioPeriodo usado, QT_DIA_APROX será dataAlvo - DT_APROX - ```(EXEMPLO: 04/03/2025)```

- ❌ **FALHA**: Pular produto.

</details>