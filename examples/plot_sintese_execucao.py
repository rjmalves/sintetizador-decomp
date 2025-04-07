"""
========================================
Síntese da Execução
========================================
"""

# %%
# Para realizar a síntese da execução de um caso do DECOMP é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Por exemplo, para se realizar a
# síntese de tempo de execução, é necessario o `decomp.tim`, para a síntese da convergência,
# o `relato.rvX` e, para a síntese de inviabilidades, o `inviab_unic.rvX`. Neste contexto,
# basta fazer::
#
#    $ sintetizador-decomp execucao
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2025-04-07 21:02:55,581 INFO: # Realizando síntese da EXECUÇÃO #
#    >>> 2025-04-07 21:02:55,581 INFO: Realizando síntese de PROGRAMA
#    >>> 2025-04-07 21:02:55,586 INFO: Tempo para sintese de PROGRAMA: 0.00 s
#    >>> 2025-04-07 21:02:55,586 INFO: Realizando síntese de VERSAO
#    >>> 2025-04-07 21:02:55,586 INFO: Lendo arquivo dec_oper_sist.csv
#    >>> 2025-04-07 21:02:55,674 INFO: Tempo para sintese de VERSAO: 0.09 s
#    >>> 2025-04-07 21:02:55,674 INFO: Realizando síntese de TITULO
#    >>> 2025-04-07 21:02:55,684 INFO: Lendo arquivo dadger.rv0
#    >>> 2025-04-07 21:02:56,004 INFO: Tempo para sintese de TITULO: 0.33 s
#    >>> 2025-04-07 21:02:56,004 INFO: Realizando síntese de CONVERGENCIA
#    >>> 2025-04-07 21:02:56,004 INFO: Lendo arquivo relato.rv0
#    >>> 2025-04-07 21:02:56,692 INFO: Tempo para sintese de CONVERGENCIA: 0.69 s
#    >>> 2025-04-07 21:02:56,692 INFO: Realizando síntese de TEMPO
#    >>> 2025-04-07 21:02:56,692 INFO: Lendo arquivo decomp.tim
#    >>> 2025-04-07 21:02:56,697 INFO: Tempo para sintese de TEMPO: 0.01 s
#    >>> 2025-04-07 21:02:56,697 INFO: Realizando síntese de INVIABILIDADES
#    >>> 2025-04-07 21:02:56,697 INFO: Lendo arquivo inviab_unic.rv0
#    >>> 2025-04-07 21:02:56,705 INFO: Lendo arquivo hidr.dat
#    >>> 2025-04-07 21:02:58,220 INFO: Tempo para sintese de INVIABILIDADES: 1.52 s
#    >>> 2025-04-07 21:02:58,221 INFO: Realizando síntese de CUSTOS
#    >>> 2025-04-07 21:02:58,231 INFO: Tempo para sintese de CUSTOS: 0.01 s
#    >>> 2025-04-07 21:02:58,236 INFO: Tempo para sintese da execucao: 2.65 s
#    >>> 2025-04-07 21:02:58,236 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
from datetime import timedelta
import pandas as pd

convergencia = pd.read_parquet("sintese/CONVERGENCIA.parquet")
custos = pd.read_parquet("sintese/CUSTOS.parquet")
tempo = pd.read_parquet("sintese/TEMPO.parquet")

# %%
# O formato dos dados de CONVERGÊNCIA:
print(convergencia.head(10))

# %%
# O formato dos dados de CUSTOS:
print(custos)

# %%
# O formato dos dados de TEMPO:
print(tempo)

# %%
# Cada arquivo pode ser visualizado de diferentes maneiras, a depender da aplicação.
# Por exemplo, é comum avaliar a convergência do modelo através da variação do gap.

fig = px.line(
    convergencia,
    x="iteracao",
    y="delta_zinf",
)
fig

# %%
# Quando se analisam os custos de cada fonte, geralmente são feitos gráficos de barras
# empilhadas ou setores:

fig = px.pie(
    custos.loc[custos["valor_esperado"] > 0],
    values="valor_esperado",
    names="parcela",
)
fig

# %%
# Uma abordagem semelhante é utilizada na análise do tempo de execução:

tempo["tempo"] = pd.to_timedelta(tempo["tempo"], unit="s") / timedelta(hours=1)
tempo["label"] = [str(timedelta(hours=d)) for d in tempo["tempo"].tolist()]
fig = px.bar(
    tempo,
    x="etapa",
    y="tempo",
    text="label",
    barmode="group",
)
fig
