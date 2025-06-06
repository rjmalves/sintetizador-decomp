Como contribuir?
=================

O módulo `idecomp` e dependências de desenvolvimento
------------------------------------------------------------

O módulo *idecomp* é desenvolvido considerando o framework proposto no módulo `cfinterface <https://github.com/rjmalves/cfi>`_. Este fornece para o desenvolvedor
uma modelagem de cada arquivo de entrada e saída do modelo DECOMP em uma classe específica.

A leitura de arquivos de entrada utilizando o módulo *idecomp* é sempre feita através do método *read*
de cada classe, que atua como construtor.

........

No *sintetizador-decomp* a dependência do módulo *idecomp* é concentrada na classe Deck, que fornece
objetos nativos, DataFrames ou arrays para as demais partes da aplicação.

Para instalar as dependências de desenvolvimento, incluindo as necessárias para a geração automática do site::
    
    $ git clone https://github.com/rjmalves/sintetizador-decomp.git
    $ cd sintetizador-decomp
    $ pip install .[dev]

.. warning::

    O conteúdo da documentação não deve ser movido para o repositório. Isto é feito
    automaticamente pelos scripts de CI no caso de qualquer modificação no branch `main`.


Convenções de código
---------------------

O *sintetizador-decomp* considera critérios de qualidade de código em seus scripts de Integração Contínua (CI), além de uma bateria de testes unitários.
Desta forma, não é possível realizar uma *release* de uma versão que não passe em todos os testes estabelecidos ou não
atenda aos critérios de qualidade de código impostos.

A primeira convenção é que sejam seguidas as diretrizes de sintaxe `PEP8 <https://peps.python.org/pep-0008/>`_, provenientes do guia de estilo
do autor da linguagem. Além disso, não é recomendado que existam funções muito complexas, com uma quantidade
excessiva de *branches* e *loops*, o que piora e legibilidade do código. Isto pode ser garantido através de módulos
específicos para análise de qualidade de código, como será mencionado a seguir. A única exceção é a regra `E203 <https://www.flake8rules.com/rules/E203.html>`_.

Para garantir a formatação é recomendado utilizar o módulo `ruff <https://docs.astral.sh/ruff/>`_, que realiza formatação automática e possui
integração nativa com alguns editores de texto no formato de *plugins* ou extensões. 

A segunda convenção é que seja utilizada tipagem estática. Isto é, não deve ser uitilizada uma variável em código a qual possua
tipo de dados que possa mudar durante a execução do mesmo. Além disso, não deve ser declarada uma variável cujo tipo não é possível de
ser inferido em qualquer situação, permanencendo incerto para o leitor o tipo de dados da variável a menos que seja feita uma
execução de teste do programa.


Gerenciamento do projeto utilizando o `pyproject.toml` e `uv`
--------------------------------------------------------------

Como sugerido pela `PyPA <https://www.pypa.io/en/latest/>`_, a definição de um projeto em Python, que antes era feita através do arquivo `setup.py`, que utilizava o módulo `setuptools` para definir dependências, requisitos e dados sobre o projeto, deve passar a ser feita através de um arquivo `pyproject.toml`.

Orientações sobre o conteúdo deste arquivo estão disponíveis `aqui <https://packaging.python.org/en/latest/tutorials/packaging-projects/>`_, e ele é facilmente extendido conforme novas ferramentas são adicionadas ao projeto, criando um único arquivo para configurar todo o projeto.

Para auxiliar a realizar tarefas como instalar localmente, adicionar uma nova dependência, executar um comando dentro de um ambiente específico, dentre outros, recomenda-se utilizar ferramentas específicas para isso, como o `uv <https://docs.astral.sh/uv/guides/projects/>`_.


Procedimentos de teste
-----------------------

O *sintetizador-decomp* realiza testes utilizando o pacote de testes de Python `pytest <https://pytest.org>`_
e controle da qualidade de código com `ruff <https://docs.astral.sh/ruff/>`_.
A tipagem estática é garantida através do uso de `mypy <http://mypy-lang.org/>`_
, que é sempre executado nos scripts de Integração Contínua (CI).

Antes de realizar um ``git push`` é recomendado que se realize estes três procedimentos
descritos, que serão novamente executados pelo ambiente de CI. Por exemplo, através do `uv`::

    $ uv run pytest ./tests
    $ uv run mypy ./app
    $ uv run ruff check ./app
