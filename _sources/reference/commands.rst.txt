.. _comandos:

Comandos
=========

Categorias de Síntese
-----------------------

O `sintetizador-decomp` está disponível como uma ferramenta CLI. Para visualizar quais comandos este pode realizar,
que estão associados aos tipos de sínteses, basta fazer::

    $ sintetizador-decomp --help

A saída observada deve ser::

    >>> Usage: main.py [OPTIONS] COMMAND [ARGS]...
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

    $ sintetizador-decomp operacao CMO_SBM_EST EARMF_SIN_EST GTER_SBM_PAT

O formato de escrita padrão das sínteses é `PARQUET <https://www.databricks.com/glossary/what-is-parquet>`, que é um formato eficiente
de armazenamento de dados tabulares para aplicações de *big data*.

Caso seja desejado, é possível forçar a saída das sínteses através do argumento opcional `--formato`, para qualquer categoria de síntese::

    $ sintetizador-decomp execucao --formato CSV

