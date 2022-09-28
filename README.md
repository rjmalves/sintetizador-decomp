# sintetizador-decomp
Programa auxiliar para realizar a síntese de dados do programa DECOMP em arquivos ou banco de dados.


## Modelo Unificado de Dados

O `sintetizador-decomp` busca organizar as informações de entrada e saída do modelo DECOMP em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

No momento são tratadas apenas informações de saída. Desta forma, foram criadas as categorias:

### Sistema

Informações da representação do sistema existente e alvo da otimização (TODO)

### Execução

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc. (TODO)

|          VARIÁVEL          |     MNEMÔNICO     |
| -------------------------- | ----------------- |
| Tempo de Execução          |       TEMPO       |
| Convergência               |   CONVERGENCIA    |
| Inviabilidades             |       INVIAB      |

Para síntese da informações da execução, as chaves de dados a serem sintetizados contém apenas os nomes das variáveis.

### Cenários

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização. (TODO)

|          VARIÁVEL          |     MNEMÔNICO     |
| -------------------------- | ----------------- |
|       Probabilidades       |   PROBABILIDADES  |

### Política

Informações sobre a política operativa construída (ou lida) pelo modelo (TODO)

### Operação

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de três especificações: 

#### Variável


|          VARIÁVEL                 |  MNEMÔNICO |
| --------------------------        | ---------- |
| Custo de Operação                 |    COP     |
| Custo Futuro                      |    CFU     |
| Custo Marginal de Operação        |    CMO     |
| Valor da Água                     |    VAGUA   |
| Custo da Geração Térmica          |    CTER    |
| Energia Natural Afluente Absoluta |    ENAA    |
| Energia Natural Afluente MLT      |    ENAM    |
| Energia Armazenada Inicial        |    EARMI   |
| Energia Armazenada Inicial (%)    |    EARPI   |
| Energia Armazenada Final          |    EARMF   |
| Energia Armazenada Final (%)      |    EARPF   |
| Geração Hidráulica                |    GHID    |
| Geração Térmica                   |    GTER    |
| Geração Eólica                    |    GEOL    |
| Energia Vertida                   |    EVER    |
| Energia Vertida Turbinável        |    EVERT   |
| Energia Vertida Não-Turbinável    |    EVERNT  |
| Vazão Afluente                    |    QAFL    |
| Vazão Defluente                   |    QDEF    |
| Vazão Incremental                 |    QINC    |
| Vazão Turbinada                   |    QTUR    |
| Velocidade do Vento               |    VENTO   |
| Vazão Vertida                     |    VVER    |
| Déficit                           |    DEF     |
| Intercâmbio                       |    INT     |
| Volume Armazenado Inicial         |    VARMI   |
| Volume Armazenado Inicial (%)     |    VARPI   |
| Volume Armazenado Final           |    VARMF   |
| Volume Armazenado Final (%)       |    VARPF   |
| Volume Vertido                    |    VVER    |
| Volume Turbinado                  |    VTUR    |


#### Agregação Espacial


|   AGERGAÇÃO ESPACIAL     |  MNEMÔNICO |
| ------------------------ | ---------- |
| Sistema Interligado      |     SIN    |
| Submercado               |     SBM    |
| Reservatório Equivalente |     REE    |
| Usina Hidroelétrica      |     UHE    |
| Usina Termelétrica       |     UTE    |
| Usina Eólica             |     UEE    |
| Par de Submercados       |     SBP    |


#### Agregação Temporal

|   AGERGAÇÃO TEMPORAL   |  MNEMÔNICO  |
| ---------------------- | ----------- |
| Estágio                |     EST     |
| Patamar                |     PAT     |


Vale destacar que nem todas as combinações de mnemônicos estão disponíveis para o modelo NEWAVE. Até o momento as implementações são:

|          VARIÁVEL          | AGERGAÇÃO ESPACIAL | AGREGAÇÃO TEMPORAL |
| -------------------------- | ------------------ | ------------------ |
| COP                        | SIN                | EST                |
| CFU                        | SIN                | EST                |
| CMO                        | SBM                | EST                |
| VAGUA                      |                    |                    |
| CTER                       | SIN, UTE           | EST                |
| ENAA                       | SBM, SIN           | EST                |
| ENAM                       | SBM                | EST                |
| EARMI                      | SIN, SBM           | EST                |
| EARPI                      | SIN, SBM           | EST                |
| EARMF                      | SIN, SBM           | EST                |
| EARPF                      | SIN, SBM           | EST                |
| GHID                       | UHE, SBM, SIN      | EST, PAT           |
| GTER                       | UTE, SBM, SIN      | EST, PAT           |
| GEOL                       |                    |                    |
| EVERT                      | UHE                | EST                |
| EVERNT                     | UHE                | EST                |
| QAFL                       | UHE                | EST                |
| QDEF                       | UHE                | EST                |
| QTUR                       |                    |                    |
| VENTO                      |                    |                    |
| INT                        | SBP                | EST, PAT           |
| VARMI                      |                    |                    |
| VARPI                      |                    |                    |
| VARMF                      | UHE                | EST                |
| VARPF                      | UHE                | EST                |
| VVER                       | UHE                | EST                |
| VTUR                       | UHE                | EST                |
| MER                        | SBM, SIN           | EST, PAT           |
| DEF                        | SBM, SIN           | EST, PAT           |


Exemplos de chaves de dados:
- EARPI_SBM_EST
- VARPF_UHE_EST
- GHID_UHE_PAT
- CMO_SBM_EST
- QDEF_UHE_EST


## Guia de Uso

Para usar o `sintetizador-newave`, ...