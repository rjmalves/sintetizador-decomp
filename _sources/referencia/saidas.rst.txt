.. _saidas:

Saídas
=========


Arquivos de Saída
-----------------------

Os arquivos de saída das sínteses são armazenados na pasta `sintese` do diretório de trabalho. Para cada síntese realizada, é configurados
um arquivo com metadados e um conjunto de arquivos com os dados sintetizados. Para as sínteses da operação e de cenários, além dos arquivos
com os dados brutos sintetizados, são criados arquivos com estatísticas pré-calculadas sobre os dados brutos,
permitindo análises mais rápidas.

No caso de uma síntese do sistema, são esperados os arquivos::

    $ ls sintese
    >>> EST.parquet
    >>> METADADOS_SISTEMA.parquet
    >>> PAT.parquet
    >>> REE.parquet
    >>> SBM.parquet
    >>> UHE.parquet
    >>> UTE.parquet

Para a síntese da execução::
    
    $ ls sintese
    >>> CONVERGENCIA.parquet
    >>> CUSTOS.parquet
    >>> INVIABILIDADES.parquet
    >>> METADADOS_EXECUCAO.parquet
    >>> PROGRAMA.parquet
    >>> TEMPO.parquet
    >>> TITULO.parquet
    >>> VERSAO.parquet

Para a síntese da política::
    
    $ ls sintese
    >>> CORTES_COEFICIENTES.parquet
    >>> CORTES_VARIAVEIS.parquet
    >>> METADADOS_POLITICA.parquet

Na síntese de cenários::
    
    $ ls sintese
    >>> METADADOS_CENARIOS.parquet
    >>> PROABABILIDADES.parquet

Alguns dos arquivos esperados na síntese da operação::

    $ ls sintese
    >>> CMO_SBM.parquet
    >>> COP_SIN.parquet
    >>> CTER_SIN.parquet
    >>> ...
    >>> ESTATISTICAS_OPERACAO_REE.parquet
    >>> ESTATISTICAS_OPERACAO_SBM.parquet
    >>> ESTATISTICAS_OPERACAO_SBP.parquet
    >>> ESTATISTICAS_OPERACAO_SIN.parquet
    >>> ESTATISTICAS_OPERACAO_UHE.parquet
    >>> ESTATISTICAS_OPERACAO_UTE.parquet
    >>> ...
    >>> GHID_REE.parquet
    >>> GHID_SBM.parquet
    >>> GHID_SIN.parquet
    >>> GHID_UHE.parquet
    >>> GTER_SBM.parquet
    >>> GTER_SIN.parquet
    >>> GTER_UTE.parquet
    >>> INT_SBP.parquet
    >>> MERL_SBM.parquet
    >>> MERL_SIN.parquet
    >>> ...
    >>> METADADOS_OPERACAO.parquet
    >>> QAFL_UHE.parquet
    >>> ... 
    >>> VARMF_UHE.parquet
    >>> VARMI_REE.parquet
    >>> VARMI_SBM.parquet
    >>> ...


Formato dos Metadados
-----------------------

As sínteses realizadas são armazenadas em arquivos de metadados, que também são DataFrames, no mesmo formato que foi solicitado para a saída da síntese (por padrão é utilizado o `parquet`).

Os metadados são armazenados em arquivos com o prefixo `METADADOS_` e o nome da síntese. Por exemplo, para a síntese do sistema, os metadados são armazenados em `METADADOS_SISTEMA.parquet`.

Por exemplo, em uma síntese da operação, os metadados podem ser acessados como:

    
.. code-block:: python

    import pandas as pd
    meta_df = pd.read_parquet("sintese/METADADOS_OPERACAO.parquet")
    meta_df

             chave  nome_curto_variavel                  nome_longo_variavel  ...  unidade calculado limitado
    0      CMO_SBM                  CMO           Custo Marginal de Operação  ...   R$/MWh     False    False
    1     CTER_UTE          Custo de GT             Custo de Geração Térmica  ...  10^3 R$     False     True
    2    EARMI_REE          EAR Inicial  Energia Armazenada Absoluta Inicial  ...    MWmes     False     True
    3    EARMI_SBM          EAR Inicial  Energia Armazenada Absoluta Inicial  ...    MWmes     False     True
    4    EARMI_SIN          EAR Inicial  Energia Armazenada Absoluta Inicial  ...    MWmes      True     True
    ..         ...                  ...                                  ...  ...      ...       ...      ...
    61  EVERNT_SIN  EVER Não-Turbinável       Energia Vertida Não-Turbinável  ...    MWmed      True     True
    62    EVER_SIN                 EVER                      Energia Vertida  ...    MWmed      True     True
    63    GTER_UTE                   GT                      Geração Térmica  ...    MWmed     False     True
    64     INT_SBP          Intercâmbio               Intercâmbio de Energia  ...    MWmed     False     True
    65    INTL_SBP  Intercâmbio Líquido       Intercâmbio Líquido de Energia  ...    MWmed     False    False
    
    [66 rows x 8 columns]


Formato das Estatísticas
--------------------------

A síntese da operação também produz estatísticas dos dados envolvidos. Em cada uma das sínteses, as estatísticas são armazenadas segundo diferentes premissas, dependendo geralmente
da agregação espacial dos dados.

As estatísticas são armazenadas em arquivos com o prefixo `ESTATISTICAS_` e o nome da síntese. Por exemplo, para a síntese da operação, as estatísticas são armazenadas em arquivos com prefixo `ESTATISTICAS_OPERACAO_`, sendo um arquivo por agregação espacial.

Por exemplo, em uma síntese da operação, as estatísticas podem ser acessadas como:


.. code-block:: python

    import pandas as pd
    hydro_df = pd.read_parquet("sintese/ESTATISTICAS_OPERACAO_UHE.parquet")
    hydro_df

           variavel  codigo_usina  codigo_ree  codigo_submercado  ...  duracao_patamar valor limite_inferior limite_superior
    0          EVER             1          10                  1  ...            168.0   0.2             0.0             inf
    1          EVER             1          10                  1  ...            168.0   0.2             0.0             inf
    2          EVER             1          10                  1  ...            168.0   0.2             0.0             inf
    3          EVER             1          10                  1  ...            168.0   0.2             0.0             inf
    4          EVER             1          10                  1  ...            168.0   0.2             0.0             inf
    ...         ...           ...         ...                ...  ...              ...   ...             ...             ...
    910772    VARPI           290          10                  1  ...            672.0   5.0            -inf             inf
    910773    VARPI           290          10                  1  ...            672.0   5.0            -inf             inf
    910774    VARPI           290          10                  1  ...            672.0   5.0            -inf             inf
    910775    VARPI           290          10                  1  ...            672.0   5.0            -inf             inf
    910776    VARPI           290          10                  1  ...            672.0   NaN            -inf             inf
    
    [910777 rows x 13 columns]


No arquivo de estatísticas, ao invés dos dados associados aos `N` cenários da etapa de simulação, são armazenadas as estatísticas dos dados associados a cada entidade, em cada estágio / patamar, calculadas nos cenários.
Nestes arquivos, a coluna `cenario` possui tipo `str`, assumindo valores `mean`, `std` e percentis de 5 em 5 (`min`, `p5`, ..., `p45`, `median`, `p55`, ..., `p95`, `max`).


Formato dos Dados Brutos
--------------------------

Os dados brutos também são armazenados em arquivos de mesma extensão dos demais produzidos pela síntese. Por exemplo, para a síntese da operação, os dados são armazenados em arquivos que possuem os nomes da chave identificadora da variável e da agregação espacial,
como `CMO_SBM` e `EARMF_REE`. Para uma mesma entidade, os arquivos de todas as variáveis possuem as mesmas colunas:


.. code-block:: python

    import pandas as pd
    eer_df = pd.read_parquet("sintese/EARMF_REE.parquet")
    eer_df

        codigo_ree  codigo_submercado  estagio data_inicio  ... duracao_patamar    valor  limite_inferior  limite_superior
    0            1                  1        1  2024-11-30  ...           168.0  26181.0              0.0          50958.0
    1            1                  1        2  2024-12-07  ...           168.0  25483.0              0.0          50958.0
    2            1                  1        3  2024-12-14  ...           168.0  25149.0              0.0          50958.0
    3            1                  1        4  2024-12-21  ...           168.0  24719.0              0.0          50958.0
    4            1                  1        5  2024-12-28  ...           168.0  24377.0              0.0          50958.0
    ..         ...                ...      ...         ...  ...             ...      ...              ...              ...
    79          12                  1        3  2024-12-14  ...           168.0   4974.0              0.0          11791.0
    80          12                  1        4  2024-12-21  ...           168.0   4907.0              0.0          11791.0
    81          12                  1        5  2024-12-28  ...           168.0   4833.0              0.0          11791.0
    82          12                  1        6  2025-01-04  ...           672.0   5073.0              0.0          11791.0
    83          12                  1        7  2025-02-01  ...           672.0   5824.0              0.0          11791.0
    
    [84 rows x 11 columns]
