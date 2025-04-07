"""
========================================
Síntese da Política
========================================
"""

# %%
# Para realizar a síntese da política de um caso do DECOMP é necessário estar em um diretório
# no qual estão os principais arquivos de saída do modelo. Por exemplo, para se realizar a
# síntese dos coeficientes dos cortes construídos, são necessarios os arquivos
# `dec_fcf_cortes_NNN.rvX`. Neste contexto, basta fazer::
#
#    $ sintetizador-decomp politica
#

# %%
# O sintetizador irá exibir o log da sua execução::
#
#    >>> 2025-04-07 21:11:56,394 INFO: # Realizando síntese da POLITICA #
#    >>> 2025-04-07 21:11:56,395 INFO: Realizando síntese de CORTES_COEFICIENTES
#    >>> 2025-04-07 21:11:56,396 INFO: Lendo arquivo dec_eco_discr.csv
#    >>> 2025-04-07 21:11:56,410 INFO: Lendo arquivo dadger.rv0
#    >>> 2025-04-07 21:11:56,734 INFO: Lendo arquivo dec_fcf_cortes_001.rv0
#    >>> 2025-04-07 21:11:56,803 INFO: Lendo arquivo dec_fcf_cortes_002.rv0
#    >>> 2025-04-07 21:11:56,872 INFO: Lendo arquivo dec_fcf_cortes_003.rv0
#    >>> 2025-04-07 21:11:56,942 INFO: Lendo arquivo dec_fcf_cortes_004.rv0
#    >>> 2025-04-07 21:11:57,011 INFO: Lendo arquivo dec_fcf_cortes_005.rv0
#    >>> 2025-04-07 21:11:57,102 INFO: Tempo para sintese de CORTES_COEFICIENTES: 0.71 s
#    >>> 2025-04-07 21:11:57,102 INFO: Realizando síntese de CORTES_VARIAVEIS
#    >>> 2025-04-07 21:11:57,109 INFO: Tempo para sintese de CORTES_VARIAVEIS: 0.01 s
#    >>> 2025-04-07 21:11:57,112 INFO: Tempo para sintese da politica: 0.72 s
#    >>> 2025-04-07 21:11:57,112 INFO: # Fim da síntese #

# %%
# Os arquivos serão salvos no subdiretório `sintese`. Para realizar o processamento,
# pode ser utilizado o próprio `python`:
import plotly.express as px
import pandas as pd

variaveis = pd.read_parquet("sintese/CORTES_VARIAVEIS.parquet")
coeficientes = pd.read_parquet("sintese/CORTES_COEFICIENTES.parquet")

# %%
# O formato dos dados de VARIAVEIS, que contém os dados referentes aos tipos das variáveis
# (eixos) dos cortes construídos:
print(variaveis)

# %%
# O formato dos dados de COEFICIENTES:
print(coeficientes.head(10))


# %%
# Cada dado pode ser visualizado de diferentes maneiras, a depender da aplicação.

# Por exemplo, é comum avaliar o comportamento dos coeficientes relativos ao volume
# armazenado de uma usina hidrelétrica (multiplicador do volume versus estado consultado)
# durante a construção da política do modelo para determinado estágio.

nome_coeficiente = "VARM"
tipo_coeficiente = variaveis.loc[
    variaveis["nome_curto_coeficiente"] == nome_coeficiente, "tipo_coeficiente"
].iloc[0]
uhe = 6
estagio = 1
coeficientes_varm = coeficientes.loc[
    (coeficientes["tipo_coeficiente"] == tipo_coeficiente)
    & (coeficientes["indice_entidade"] == uhe)
    & (coeficientes["estagio"] == 1)
]

fig = px.scatter(
    coeficientes_varm, x="valor_estado", y="valor_coeficiente", color="iteracao"
)
fig
