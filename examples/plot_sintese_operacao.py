"""
========================================
Síntese da Operação
========================================
"""

# %%
# Para realizar a síntese da operação de um caso do DECOMP é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Em geral, as variáveis da operação
# são extraídas dos arquivos relato.rvX e relato2.rvX.
# Além dos arquivos dos quais são extraídas as variáveis em si, são lidos também alguns arquivos de entrada
# do modelo, como o `dadger.rvX` e `hidr.dat``. Neste contexto, basta fazer::
#
#    $ sintetizador-decomp operacao
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2025-04-07 20:04:41,917 INFO: # Realizando síntese da OPERACAO #
#    >>> .
#    >>> .
#    >>> .
#    >>> 2025-04-07 20:07:38,946 INFO: Realizando sintese de GTER_UTE
#    >>> 2025-04-07 20:07:38,981 INFO: Tempo para obtenção dos dados do dec_oper_usit: 0.03 s
#    >>> 2025-04-07 20:07:39,016 INFO: Tempo para compactacao dos dados: 0.03 s
#    >>> 2025-04-07 20:07:39,093 INFO: Tempo para calculo dos limites: 0.08 s
#    >>> 2025-04-07 20:07:39,746 INFO: Tempo para preparacao para exportacao: 0.65 s
#    >>> 2025-04-07 20:07:39,806 INFO: Tempo para exportacao dos dados: 0.06 s
#    >>> 2025-04-07 20:07:39,806 INFO: Tempo para sintese de GTER_UTE: 0.86 s
#    >>> 2025-04-07 20:07:39,806 INFO: Realizando sintese de INT_SBP
#    >>> 2025-04-07 20:07:39,807 INFO: Lendo arquivo dec_oper_interc.csv
#    >>> 2025-04-07 20:07:46,525 INFO: Tempo para obtenção dos dados do dec_oper_interc: 6.72 s
#    >>> 2025-04-07 20:07:46,531 INFO: Tempo para compactacao dos dados: 0.01 s
#    >>> 2025-04-07 20:07:46,546 INFO: Tempo para calculo dos limites: 0.01 s
#    >>> 2025-04-07 20:07:46,657 INFO: Tempo para preparacao para exportacao: 0.11 s
#    >>> 2025-04-07 20:07:46,670 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2025-04-07 20:07:46,670 INFO: Tempo para sintese de INT_SBP: 6.86 s
#    >>> 2025-04-07 20:07:46,670 INFO: Realizando sintese de INTL_SBP
#    >>> 2025-04-07 20:07:46,793 INFO: Tempo para obtenção dos dados do dec_oper_interc: 0.12 s
#    >>> 2025-04-07 20:07:46,797 INFO: Tempo para compactacao dos dados: 0.00 s
#    >>> 2025-04-07 20:07:46,798 INFO: Tempo para calculo dos limites: 0.00 s
#    >>> 2025-04-07 20:07:46,862 INFO: Tempo para preparacao para exportacao: 0.06 s
#    >>> 2025-04-07 20:07:46,870 INFO: Tempo para exportacao dos dados: 0.01 s
#    >>> 2025-04-07 20:07:46,870 INFO: Tempo para sintese de INTL_SBP: 0.20 s
#    >>> 2025-04-07 20:07:47,894 INFO: Tempo para sintese da operacao: 185.98 s
#    >>> 2025-04-07 20:07:47,894 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# %%
# Para a síntese da operação é produzido um arquivo com as informações das sínteses
# que foram realizadas:
metadados = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
print(metadados.head(10))

# %%
# Os arquivos com os nomes das sínteses de operação armazenam os dados
# de todos os cenários simulados.
cmo = pd.read_parquet("sintese/CMO_SBM.parquet")
earmf = pd.read_parquet("sintese/EARMF_SIN.parquet")
varpf = pd.read_parquet("sintese/VARPF_UHE.parquet")

# %%
# O formato dos dados de CMO:
print(cmo.head(10))

# %%
# Os tipos de dados da síntese de CMO:
cmo.dtypes

# %%
# O formato dos dados de EARMF:
print(earmf.head(10))

# %%
# Os tipos de dados da síntese de EARMF:
earmf.dtypes

# %%
# O formato dos dados de VARPF:
print(varpf.head(10))

# %%
# Os tipos de dados da síntese de VARPF:
varpf.dtypes

# %%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `data_inicio`, `data_fim`, `cenario`, `patamar` e `valor`. A depender se o arquivo é
# relativo a uma agregação espacial diferente de todo o SIN ou agregação temporal
# diferente do valor médio por estágio, existirão outras colunas adicionais para determinar
# de qual subconjunto da agregação o dado pertence. Por exemplo, no arquivo da síntese de
# CMO_SBM, existe uma coluna adicional de nome `codigo_submercado`.

# %%
# A coluna de cenários contém não somente inteiros de 1 a N, onde N é o número de séries da
# simulação final do modelo.

cenarios = earmf["cenario"].unique().tolist()
print(cenarios)

# %%
# Através das estatísticas é possível fazer um gráfico de caixas, para ilustrar a dispersão
# da variável da operação com os cenários:
fig = px.box(earmf, x="data_inicio", y="valor")
fig

# %%
# Para variáveis da operação que possuam diferentes subconjuntos, como os submercados, podem ser utilizados
# gráficos de violino para avaliação da dispersão nos cenários do 2º mês:
ultimo_estagio = cmo["estagio"].max()
cmos_2omes = cmo.loc[cmo["estagio"] == ultimo_estagio]
fig = px.violin(
    cmos_2omes,
    y="valor",
    color="codigo_submercado",
    box=True,
)
fig


# %%
# Para dados por UHE, como o número de subconjuntos é muito grande, é possível
# fazer um subconjunto dos elementos de interesse para a visualização:
varpf_1omes = varpf.loc[
    (varpf["estagio"] < ultimo_estagio)
    & varpf["codigo_usina"].isin([6, 169, 251, 275])
]
fig = px.line(
    varpf_1omes,
    x="data_inicio",
    y="valor",
    facet_col_wrap=2,
    facet_col="codigo_usina",
)
fig

# %%
# Além dos arquivos com as sínteses dos cenários, estão disponíveis também os arquivos
# que agregam estatísticas das previsões:

estatisticas_uhe = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
print(estatisticas_uhe.head(10))

# %%
# As informações dos arquivos de estatísticas são:
print(estatisticas_uhe.columns)

# %%
# Um arquivo único é gerado para as estatísticas de todas as variáveis, agregadas
# por cada elemento do sistema.:
print(estatisticas_uhe["variavel"].unique())

# %%
# As estatísticas disponíveis são os valores mínimos, máximos, médios e quantis a cada
# 5 percentis para cada variável de cada elemento de sistema. No caso das UHEs:
print(estatisticas_uhe["cenario"].unique())


# %%
# Através das estatísticas é possível fazer um gráfico de quantis, para ilustrar a dispersão
# da variável da operação com os cenários:
estatisticas_sin = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_SIN.parquet")
percentis = [col for col in estatisticas_sin["cenario"].unique() if "p" in col]
estatisticas_earmf = estatisticas_sin.loc[
    estatisticas_sin["variavel"] == "EARMF"
]
fig = go.Figure()
for p in percentis:
    earmf_p = estatisticas_earmf.loc[estatisticas_earmf["cenario"] == f"{p}"]
    print(earmf_p)
    fig.add_trace(
        go.Scatter(
            x=earmf_p["data_fim"],
            y=earmf_p["valor"],
            line={
                "color": "rgba(66, 135, 245, 0.3)",
                "width": 2,
            },
            name=f"p{p}",
            showlegend=False,
        )
    )
fig

# %%
# Também é possível fazer uma análise por meio de gráficos de linhas com áreas sombreadas,
# para ilustrar a cobertura dos cenários no domínio da variável:
fig = go.Figure()
earm_mean = estatisticas_earmf.loc[estatisticas_earmf["cenario"] == "mean"]
earm_max = estatisticas_earmf.loc[estatisticas_earmf["cenario"] == "max"]
earm_min = estatisticas_earmf.loc[estatisticas_earmf["cenario"] == "min"]
fig.add_trace(
    go.Scatter(
        x=earm_mean["data_fim"],
        y=earm_mean["valor"],
        line={
            "color": "rgba(66, 135, 245, 0.9)",
            "width": 4,
        },
        name="mean",
    )
)
fig.add_trace(
    go.Scatter(
        x=earm_min["data_fim"],
        y=earm_min["valor"],
        line={
            "color": "rgba(66, 135, 245, 0.9)",
            "width": 4,
        },
        line_color="rgba(66, 135, 245, 0.3)",
        fillcolor="rgba(66, 135, 245, 0.3)",
        name="min",
    )
)
fig.add_trace(
    go.Scatter(
        x=earm_max["data_fim"],
        y=earm_max["valor"],
        line={
            "color": "rgba(66, 135, 245, 0.9)",
            "width": 4,
        },
        line_color="rgba(66, 135, 245, 0.3)",
        fill="tonexty",
        name="max",
    )
)
fig
