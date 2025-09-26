Tutorial
============


Categorias de Síntese
-----------------------

O `sintetizador-decomp` está disponível como uma ferramenta CLI. Para visualizar quais comandos este pode realizar,
que estão associados aos tipos de sínteses, basta fazer::

    $ sintetizador-decomp --help

A saída observada deve ser::

    >>> Usage: sintetizador-decomp [OPTIONS] COMMAND [ARGS]...
    >>> 
    >>>   Aplicação para realizar a síntese de informações em um modelo unificado de
    >>>   dados para o DECOMP.
    >>> 
    >>> Options:
    >>>   --help  Show this message and exit.
    >>> 
    >>> Commands:
    >>>   cenarios  Realiza a síntese dos dados de cenários do DECOMP.
    >>>   completa  Realiza a síntese completa do DECOMP.
    >>>   execucao  Realiza a síntese dos dados da execução do DECOMP.
    >>>   limpeza   Realiza a limpeza dos dados resultantes de uma síntese.
    >>>   operacao  Realiza a síntese dos dados da operação do DECOMP.
    >>>   sistema   Realiza a síntese dos dados do sistema do DECOMP.

Além disso, cada um dos comandos possui um menu específico, que pode ser visto com, por exemplo::

    $ sintetizador-decomp operacao --help

Que deve ter como saída::

    >>> Usage: sintetizador-decomp operacao [OPTIONS] [VARIAVEIS]...
    >>> 
    >>>   Realiza a síntese dos dados da operação do DECOMP.
    >>> 
    >>> Options:
    >>>   --formato TEXT  formato para escrita da síntese
    >>>   --help          Show this message and exit.


Argumentos Existentes
-----------------------

Para realizar a síntese completa do caso, está disponível o comando `completa`, que realizará toda a síntese possível::

    $ sintetizador-decomp completa 

Se for desejado não realizar a síntese completa, mas apenas de alguns dos elementos, é possível chamar cada elemento a ser sintetizado::

    $ sintetizador-decomp operacao CMO_SBM EARMF_SIN GTER_SBM

O formato de escrita padrão das sínteses é `PARQUET <https://www.databricks.com/glossary/what-is-parquet>`, que é um formato eficiente
de armazenamento de dados tabulares para aplicações de *big data*.

Caso seja desejado, é possível forçar a saída das sínteses através do argumento opcional `--formato`, para qualquer categoria de síntese::

    $ sintetizador-decomp execucao --formato CSV

No caso das sínteses da operação e de cenários, é possível paralelizar a leitura dos arquivos através do argumento opcional `--processadores`::

    $ sintetizador-decomp operacao --processadores 8
    $ sintetizador-decomp cenarios --processadores 8

A síntese completa também aceita o paralelismo, aplicando-o a todas as categorias de síntese que são suportadas::

    $ sintetizador-decomp completa --processadores 24



Exemplo de Uso
------------------


Um exemplo de chamada ao programa para realizar a síntese da operação de um caso do NEWAVE é o seguinte::

    $ sintetizador-decomp operacao

O log observado no terminal deve ser semelhante a::

    >>> 2025-01-13 09:11:13,412 INFO: # Realizando síntese da OPERACAO #
    >>> 2025-01-13 09:11:13,413 INFO: Variáveis: [CMO_SBM, CTER_UTE, CTER_SIN, ...]
    >>> 2025-01-13 09:11:13,414 INFO: Realizando sintese de CMO_SBM
    >>> 2025-01-13 09:11:13,414 INFO: Lendo arquivo dec_oper_sist.csv
    >>> 2025-01-13 09:11:13,418 INFO: Lendo arquivo dec_eco_discr.csv
    >>> 2025-01-13 09:11:13,427 INFO: Lendo arquivo dadger.rv0
    >>> 2025-01-13 09:11:13,703 INFO: Tempo para obtenção dos dados do dec_oper_sist: 0.29 s
    >>> 2025-01-13 09:11:13,704 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2025-01-13 09:11:13,704 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2025-01-13 09:11:13,704 INFO: Lendo arquivo vazoes.rv0
    >>> 2025-01-13 09:11:13,735 INFO: Tempo para preparacao para exportacao: 0.03 s
    >>> 2025-01-13 09:11:13,739 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2025-01-13 09:11:13,739 INFO: Tempo para sintese de CMO_SBM: 0.33 s
    >>> ...
    >>> 2025-01-13 09:10:09,276 INFO: Realizando sintese de GTER_UTE
    >>> 2025-01-13 09:10:09,277 INFO: Tempo para obtenção dos dados do dec_oper_usit: 0.00 s
    >>> 2025-01-13 09:10:09,278 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2025-01-13 09:10:09,280 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2025-01-13 09:10:09,566 INFO: Tempo para preparacao para exportacao: 0.29 s
    >>> 2025-01-13 09:10:09,569 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2025-01-13 09:10:09,569 INFO: Tempo para sintese de GTER_UTE: 0.29 s
    >>> 2025-01-13 09:10:09,796 INFO: Realizando sintese de INTL_SBP
    >>> 2025-01-13 09:10:09,810 INFO: Tempo para obtenção dos dados do dec_oper_interc: 0.01 s
    >>> 2025-01-13 09:10:09,811 INFO: Tempo para compactacao dos dados: 0.00 s
    >>> 2025-01-13 09:10:09,811 INFO: Tempo para calculo dos limites: 0.00 s
    >>> 2025-01-13 09:10:09,843 INFO: Tempo para preparacao para exportacao: 0.03 s
    >>> 2025-01-13 09:10:09,844 INFO: Tempo para exportacao dos dados: 0.00 s
    >>> 2025-01-13 09:10:09,845 INFO: Tempo para sintese de INTL_SBP: 0.05 s
    >>> 2025-01-13 09:10:10,437 INFO: Tempo para sintese da operacao: 19.44 s
    >>> 2025-01-13 09:10:10,437 INFO: # Fim da síntese #