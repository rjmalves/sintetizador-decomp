# v2.0.0

- Suporte a Python 3.8 descontinuado. Apenas versões de Python >= 3.10 são suportadas nos ambientes de CI e tem garantia de reprodutibilidade.
- Descontinuado o uso do `pylama` como linter para garantir padrões PEP de código devido à falta de suporte em Python >= 3.12. Adoção do [ruff](https://github.com/astral-sh/ruff) em substituição.
- Refatoração dos processos de síntese, contemplando reuso de código e padronização de nomes de funções e variáveis.
- Opção de exportação de saídas `PARQUET` não realiza mais a compressão em `gzip` automaticamente, adotando o `snappy` (padrão do Arrow). A extensão dos arquivos passa a ser apenas `.parquet`.
- Colunas do tipo `datetime` agora garante que a informação de fuso seja `UTC`, permitindo maior compatibilidade na leitura em outras implementações do Arrow. [#43](https://github.com/rjmalves/sintetizador-newave/issues/43)
- Colunas dos DataFrames de síntese padronizadas para `snake_case`.
- Entidades passam a ser indexadas pelos seus códigos ao invés de nomes nos DataFrames das sínteses da operação e de cenários (`usina` -> `codigo_usina`, etc.). A síntese com opção `sistema` contem o mapeamento entre códigos e nomes.
- Estatísticas calculadas a partir dos cenários de cada variável, para cada entidade, em um determinado estágio, passam a ser salvas em saídas especíicas (`ESTATISTICAS_OPERACAO_UHE.parquet`, por exemplo).
- Uso do módulo [numba](https://numba.pydata.org/) como dependência opcional para aceleração de operações com DataFrames.
- Substituída a divisão da síntese da operação utilizando agregação temporal (`EST` e `PAT`) pela inclusão sempre das colunas `patamar` e `duracao_patamar`, onde `patamar = 0` representa o valor médio do estágio [#12](https://github.com/rjmalves/sintetizador-decomp/issues/12)
- As sínteses agora produzem sempre como saída um arquivo de metadados, com informações sobre as sínteses que foram geradas (`METADADOS_OPERACAO.parquet`, `METADADOS_SISTEMA.parquet`, etc.) [#13](https://github.com/rjmalves/sintetizador-decomp/issues/13)
- Implementado suporte para uso do caractere de `wildcard` (`*`) na especificação das sínteses desejadas via CLI [#14](https://github.com/rjmalves/sintetizador-decomp/issues/14)
- Implementado o cálculo de limites para variáveis da síntese da operação que sejam limitadas inferior ou superiormente (colunas `limite_inferior` e `limite_superior`) [#11](https://github.com/rjmalves/sintetizador-decomp/issues/11)
- Dados das sínteses de operação e cenários que sejam agrupados por entidades menores contém os códigos dos conjuntos que englobam estas entidades, permitindo agrupamentos arbitrários pelo usuário (ex. sínteses por UHE também contém colunas `codigo_ree` e `codigo_submercado`) [#10](https://github.com/rjmalves/sintetizador-decomp/issues/10)
- Implementada síntese de Energia Armazenada por UHE, com cálculo feito na aplicação de síntese (`EARMI_UHE`, `EARMF_UHE`) [#15](https://github.com/rjmalves/sintetizador-decomp/issues/15)
- Logging do processo de síntese melhorado e resumido, incluindo os tempos gastos em cada etapa do processo
- Criação da abstração `Deck` que centraliza as conversões de formato e implementação de cálculos já realizados pelo modelo quando necessários para padronização do restante dos módulos de síntese
- Dependência da [idecomp](https://github.com/rjmalves/idecomp) atualizada para 1.6.0.
- Implementada as variáveis de geração de usinas não simuladas para a síntese da operação: `GUNS`, `GUNSD`, `CUNS`.
- Implementada síntese da política construída pelo DECOMP, com as saídas `CORTES_COEFICIENTES.parquet`e `CORTES_VARIAVEIS.parquet`.


# v1.0.0

- Primeira major release
- Compatível com `idecomp` versão 0.0.60
- Sínteses refatoradas para, sempre que possível, obter dados dos arquivos `dec_oper_XXX.csv`
- Retrocompatibilidade a partir da versão `31.0.2` do modelo DECOMP
