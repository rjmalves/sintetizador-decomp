.. _model:

Modelo Unificado de Dados
############################

O `sintetizador-decomp` busca organizar as informações de entrada e saída do modelo DECOMP em um modelo padronizado para lidar com os modelos do planejamento energético do SIN.

Desta forma, foram criadas as categorias:


Sistema
********

Informações da representação do sistema existente e alvo da otimização.

.. list-table:: Dados do Sistema
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - `MNEMÔNICO`
   * - Estágios
     - `EST`
   * - Patamares
     - `PAT`
   * - Submercados
     - `SBM`
   * - Reservatórios Equivalentes de Energia
     - `REE`
   * - Parques Eólicos Equivalentes
     - `PEE`
   * - Usina Termoelétrica
     - `UTE`
   * - Usina Hidroelétrica
     - `UHE`

Execução
********

Informações da execução do modelo, como ambiente escolhido, recursos computacionais disponíveis, convergência, tempo gasto, etc. 

.. list-table:: Dados da Execução
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Composição de Custos
     - `CUSTOS`
   * - Tempo de Execução
     - `TEMPO`
   * - Convergência
     - `CONVERGENCIA`
   * - Inviabilidades do caso
     - `INVIABILIDADES`
   * - Recursos Computacionais do Job
     - `RECURSOS_JOB`
   * - Recursos Computacionais do Cluster
     - `RECURSOS_CLUSTER`

Os mnemônicos `RECURSOS_JOB` e `RECURSOS_CLUSTER` dependem de arquivos que não são gerados automaticamente pelo modelo DECOMP,
e sim por outras ferramentas adicionais. Portanto, não devem ser utilizados em ambientes recentemente configurados.

Cenários
*********

Informações sobre os cenários visitados (gerados, fornecidos, processados, etc.) durante o processo de otimização.

.. list-table:: Dados de Cenários
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Probabilidades dos cenários
     - `PROBABILIDADES`

Política
*********

Informações sobre a política operativa construída (ou lida) pelo modelo (TODO)

Operação
*********

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de três especificações:

Variável
=========

A variável informa a grandeza que é modelada e fornecida como saída da operação pelo modelo.

.. list-table:: Variáveis da Operação
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Corte de Geração Eólica (MWMes)
     - `VEOL`
   * - Cota de Jusante (m)
     - `HJUS`
   * - Cota de Montante (m)
     - `HMON`
   * - Custo de Operação (Presente - 10^3 R$)
     - `COP`
   * - Custo Futuro (10^3 R$)
     - `CFU`
   * - Custo Marginal de Operação (R$/MWh)
     - `CMO`
   * - Custo da Geração Térmica (10^3 R$)
     - `CTER`
   * - Déficit (MWmed)
     - `DEF`
   * - Energia Natural Afluente Absoluta (MWmes)
     - `ENAA`
   * - Energia Armazenada Inicial (MWmes)
     - `EARMI`
   * - Energia Armazenada Inicial (%)
     - `EARPI`
   * - Energia Armazenada Final (MWmes)
     - `EARMF`
   * - Energia Armazenada Final (%)
     - `EARPF`
   * - Energia Vertida (MWmed)
     - `EVER`
   * - Energia Vertida Turbinável (MWmed)
     - `EVERT`
   * - Energia Vertida Não-Turbinável (MWmed)
     - `EVERNT`
   * - Energia Vertida em Reservatórios (MWmed)
     - `EVERR`
   * - Energia Vertida Turbinável em Reservatórios (MWmed)
     - `EVERRT`
   * - Energia Vertida Não-Turbinável em Reservatórios (MWmed)
     - `EVERRNT`
   * - Energia Vertida em Fio d'Água (MWmed)
     - `EVERF`
   * - Energia Vertida Turbinável em Fio d'Água (MWmed)
     - `EVERFT`
   * - Energia Vertida Não-Turbinável em Fio d'Água (MWmed)
     - `EVERFNT`
   * - Geração Hidráulica (MWmed)
     - `GHID`
   * - Geração Térmica (MWmed)
     - `GTER`
   * - Geração Eólica (MWmed)
     - `GEOL`
   * - Intercâmbio (MWmed)
     - `INT`
   * - Mercado de Energia (MWmed)
     - `MER`
   * - Mercado de Energia Líquido (MWmes)
     - `MERL`
   * - Queda Líquida (m)
     - `HLIQ`
   * - Valor da Água (R$/hm3)
     - `VAGUA`
   * - Vazão Afluente (m3/s)
     - `QAFL`
   * - Vazão Defluente (m3/s)
     - `QDEF`
   * - Vazão Desviada (m3/s)
     - `QDES`
   * - Vazão Incremental (m3/s)
     - `QINC`
   * - Vazão Retirada (m3/s)
     - `QRET`
   * - Vazão Turbinada (m3/s)
     - `QTUR`
   * - Vazão Vertida (m3/s)
     - `QVER`
   * - Velocidade do Vento (m/s)
     - `VENTO`
   * - Volume Armazenado Inicial (hm3)
     - `VARMI`
   * - Volume Armazenado Inicial (%)
     - `VARPI`
   * - Volume Armazenado Final (hm3)
     - `VARMF`
   * - Volume Armazenado Final (%)
     - `VARPF`
   * - Volume Afluente (hm3)
     - `VAFL`
   * - Volume Defluente (hm3)
     - `VDEF`
   * - Volume Desviado (hm3)
     - `VDES`
   * - Volume Incremental (hm3)
     - `VINC`
   * - Volume Retirado (hm3)
     - `VRET`
   * - Volume Turbinado (hm3)
     - `VTUR`
   * - Volume Vertido (hm3)
     - `VVER`

Agregação Espacial
===================

A agregação espacial informa o nível de agregação da variável em questão
em relação ao conjunto de elementos do sistema.

.. list-table:: Possíveis Agregações Espaciais
   :widths: 50 10
   :header-rows: 1

   * - AGREGAÇÂO
     - MNEMÔNICO
   * - Sistema Interligado
     - `SIN`
   * - Submercado
     - `SBM`
   * - Reservatório Equivalente
     - `REE`
   * - Usina Hidroelétrica
     - `UHE`
   * - Usina Termelétrica
     - `UTE`
   * - Parque Eólico Equivalente
     - `PEE`
   * - Par de Submercados
     - `SBP`


Agregação Temporal
===================

A agregação espacial informa o nível de agregação da variável em questão em relação
à discretização temporal (médio diário, semanal, mensal, por patamar, etc.).

.. list-table:: Possíveis Agregações Temporais
   :widths: 50 10
   :header-rows: 1

   * - AGREGAÇÂO
     - MNEMÔNICO
   * - Estágio
     - `EST`
   * - Patamar
     - `PAT`


Estado do Desenvolvimento
***************************

Todas as variáveis das categorias `Sistema`, `Execução`, `Cenários` e `Política` que são listadas
e estão presentes no modelo DECOMP, estão disponíveis para uso no sintetizador.

Já para a categoria de operação, nem todas as combinações de agregações espaciais, temporais e variáveis
fazem sentido, ou especialmente são modeladas ou possíveis de se obter no DECOMP. Desta forma,
o estado do desenvolvimento é listado a seguir, onde se encontram as combinações de sínteses da operação
que estão disponíveis no modelo.

.. list-table:: Sínteses da Operação Existentes
   :widths: 50 10 10
   :header-rows: 1

   * - VARIÁVEL
     - AGREGAÇÃO ESPACIAL
     - AGREGAÇÃO TEMPORAL
   * - `VEOL`
     - 
     - 
   * - `HJUS`
     - 
     - 
   * - `HMON`
     - 
     - 
   * - `COP`
     - `SIN`
     - `EST`
   * - `CFU`
     - `SIN`
     - `EST`
   * - `CMO`
     - `SBM`
     - `EST`, `PAT`
   * - `CTER`
     - `SIN`, `UTE`
     - `EST`
   * - `DEF`
     - `SIN`, `SBM`
     - `EST`, `PAT`
   * - `ENAA`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EARMI`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EARPI`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EARMF`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EARPF`
     - `SIN`, `SBM`, `REE`
     - `EST`
   * - `EVER`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `EVERT`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `EVERNT`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `EVERF`
     - 
     - 
   * - `EVERR`
     - 
     - 
   * - `EVERFT`
     - 
     - 
   * - `EVERFNT`
     - 
     - 
   * - `EVERRT`
     - 
     - 
   * - `EVERRNT`
     - 
     - 
   * - `GHID`
     - `SIN`, `SBM`, `UHE`
     - `EST`, `PAT`
   * - `GTER`
     - `SIN`, `SBM`, `UTE`
     - `EST`, `PAT`
   * - `GEOL`
     - `SIN`, `SBM`
     - `EST`, `PAT`
   * - `INT`
     - `SBP`
     - `EST`, `PAT`
   * - `MER`
     - `SIN`, `SBM`
     - `EST`, `PAT`
   * - `MERL`
     - `SIN`, `SBM`
     - `EST`, `PAT`
   * - `HLIQ`
     -
     -
   * - `VAGUA`
     - 
     - 
   * - `QAFL`
     - `UHE`
     - `EST`
   * - `QDES`
     -
     -
   * - `QDEF`
     - `UHE`
     - `EST`
   * - `QINC`
     - 
     - 
   * - `QRET`
     - 
     - 
   * - `QTUR`
     - `UHE`
     - `EST`
   * - `QVER`
     - `UHE`
     - `EST`
   * - `VENTO`
     - 
     -
   * - `VARMI`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `VARPI`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `VARMF`
     - `SIN`, `SBM`, `REE`, `UHE`
     - `EST`
   * - `VARPF`
     - `UHE`
     - `EST`
   * - `VAFL`
     - 
     - 
   * - `VDEF`
     - 
     - 
   * - `VDES`
     - 
     - 
   * - `VINC`
     - 
     - 
   * - `VRET`
     - 
     - 
   * - `VVER`
     - 
     - 
   * - `VTUR`
     - 
     - 

São exemplos de elementos de dados válidos para as sínteses da operação `EARPF_SBM_EST`, `VARPF_UHE_EST`, `GHID_UHE_PAT`, `CMO_SBM_EST`, dentre outras.