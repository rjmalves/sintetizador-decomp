# sintetizador-decomp

Programa auxiliar para realizar a síntese de dados do programa DECOMP em arquivos ou banco de dados.

[![tests](https://github.com/rjmalves/sintetizador-decomp/actions/workflows/main.yml/badge.svg)](https://github.com/rjmalves/sintetizador-decomp/actions/workflows/main.yml)
[![codecov](https://codecov.io/gh/rjmalves/sintetizador-decomp/graph/badge.svg?token=8KTPAR862Z)](https://codecov.io/gh/rjmalves/sintetizador-decomp)
[![docs](https://img.shields.io/badge/docs-online-blue)](https://rjmalves.github.io/sintetizador-decomp/)
[![python](https://img.shields.io/badge/python-%3E%3D%203.11-blue)](https://www.python.org/)

## Instalação

A instalação pode ser feita diretamente via `uv`:

```
$ uv pip install git+https://github.com/rjmalves/sintetizador-decomp
```

Ou através do `pip`:

```
$ pip install git+https://github.com/rjmalves/sintetizador-decomp
```

Para desenvolvimento local com todas as dependências:

```
$ git clone https://github.com/rjmalves/sintetizador-decomp
$ cd sintetizador-decomp
$ uv sync --all-extras --dev
```

## Modelo Unificado de Dados

O `sintetizador-decomp` busca organizar as informações de entrada e saída do modelo DECOMP em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

As saídas são geradas em formato Parquet e podem ser lidas com [pandas](https://pandas.pydata.org/) ou [Polars](https://pola.rs/):

```python
import pandas as pd
df = pd.read_parquet("CMARG_SBM.parquet")

import polars as pl
df = pl.read_parquet("CMARG_SBM.parquet")
```

## Comandos

O `sintetizador-decomp` é uma aplicação CLI, que pode ser utilizada diretamente no terminal após a instalação:

```
$ sintetizador-decomp completa

> 2025-01-15 10:30:12,045 INFO: # Realizando síntese da OPERACAO #
> 2025-01-15 10:30:12,078 INFO: Lendo arquivo relato.rv0
...
> 2025-01-15 10:30:45,321 INFO: # Fim da síntese #
```

Para sintetizar apenas a política construída pelo DECOMP:

```
$ sintetizador-decomp politica
```

## Documentação

Guias, tutoriais e as referências podem ser encontrados no site oficial do pacote: https://rjmalves.github.io/sintetizador-decomp
