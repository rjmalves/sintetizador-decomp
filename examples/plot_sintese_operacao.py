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
#    $ sintetizador-decomp operacao CMO_SBM_EST EARMF_SIN_EST
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
# >>> 2023-09-25 15:08:15,466 INFO: # Realizando síntese da OPERACAO #
# >>> 2023-09-25 15:08:15,466 INFO: Variáveis: [CMO_SBM_EST, EARMF_SBM_EST]
# >>> 2023-09-25 15:08:15,466 INFO: Realizando síntese de CMO_SBM_EST
# >>> 2023-09-25 15:08:15,467 INFO: Lendo arquivo dec_oper_sist.csv
# >>> 2023-09-25 15:08:15,632 INFO: Lendo arquivo dec_eco_discr.csv
# >>> 2023-09-25 15:08:15,649 INFO: Lendo arquivo dadger.rv0
# >>> 2023-09-25 15:08:18,544 INFO: Realizando síntese de EARMF_SBM_EST
# >>> 2023-09-25 15:08:18,742 INFO: # Fim da síntese #


# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

cmo = pd.read_parquet("sintese/CMO_SBM_EST.parquet.gzip")
earm = pd.read_parquet("sintese/EARMF_SIN_EST.parquet.gzip")

# %%
# O formato dos dados de CMO:
cmo.head(10)

# %%
# O formato dos dados de EARM:
earm.head(10)

# %%
# De modo geral, os arquivos das sínteses de operação sempre possuem as colunas
# `estagio`, `dataInicio`, `dataFim`, `cenario` e `valor`. A depender se o arquivo é
# relativo a uma agregação espacial diferente de todo o SIN ou agregação temporal
# diferente do valor médio por estágio, existirão outras colunas adicionais para determinar
# de qual subconjunto da agregação o dado pertence. Por exemplo, no arquivo da síntese de
# CMO_SBM_EST, existe uma coluna adicional de nome `submercado`.

# %%
# A coluna de cenários contém não somente inteiros de 1 a N, onde N é o número de séries da
# simulação final do modelo, mas também algumas outras palavras especiais, associadas a estatísticas
# processadas sobre os cenários: `min`, `max`, `mean`, `p5`, `p10`, ..., `p95`.

cenarios = earm["cenario"].unique().tolist()
cenarios_estatisticas = [
    c for c in cenarios if c not in [str(c) for c in list(range(1, 2001))]
]
print(cenarios_estatisticas)

# %%
# Através das estatísticas é possível fazer um gráfico de quantis, para ilustrar a dispersão
# da variável da operação com os cenários:
fig = go.Figure()
for p in range(10, 91, 10):
    earm_p = earm.loc[earm["cenario"] == f"p{p}"]
    fig.add_trace(
        go.Scatter(
            x=earm_p["dataFim"],
            y=earm_p["valor"],
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
earm_mean = earm.loc[earm["cenario"] == "mean"]
earm_max = earm.loc[earm["cenario"] == "max"]
earm_min = earm.loc[earm["cenario"] == "min"]
fig.add_trace(
    go.Scatter(
        x=earm_mean["dataFim"],
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
        x=earm_min["dataFim"],
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
        x=earm_max["dataFim"],
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

# %%
# Para variáveis da operação que possuam diferentes subconjuntos, como os submercados, podem ser utilizados
# gráficos de violino para avaliação da dispersão:
cenarios = [str(c) for c in list(range(1, 2001))]
cmos_cenarios = cmo.loc[
    (cmo["estagio"] == 2) & (cmo["cenario"].isin(cenarios))
]
fig = px.violin(
    cmos_cenarios,
    y="valor",
    color="submercado",
    box=True,
)
fig
