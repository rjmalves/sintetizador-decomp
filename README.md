# sintetizador-decomp
Programa auxiliar para realizar a síntese de dados do programa DECOMP em arquivos ou banco de dados.

[![tests](https://github.com/rjmalves/sintetizador-decomp/actions/workflows/main.yml/badge.svg)](https://github.com/rjmalves/sintetizador-decomp/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/rjmalves/sintetizador-decomp/graph/badge.svg?token=8KTPAR862Z)](https://codecov.io/gh/rjmalves/sintetizador-decomp)


## Instalação

A instalação pode ser feita diretamente a partir do repositório:
```
$ git clone https://github.com/rjmalves/sintetizador-decomp
$ cd sintetizador-decomp
$ python setup.py install
```

## Modelo Unificado de Dados

O `sintetizador-decomp` busca organizar as informações de entrada e saída do modelo DECOMP em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

## Comandos

O `sintetizador-decomp` é uma aplicação CLI, que pode ser utilizada diretamente no terminal após a instalação:

```
$ sintetizador-decomp completa

> 2023-02-10 02:02:05,214 INFO: # Realizando síntese da OPERACAO #
> 2023-02-10 02:02:05,225 INFO: Lendo arquivo relato.rv0
...
> 2023-02-10 02:02:06,636 INFO: # Fim da síntese #
```

## Documentação

Guias, tutoriais e as referências podem ser encontrados no site oficial do pacote: https://rjmalves.github.io/sintetizador-decomp