.. _modelo:

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
   * - Energia Natural Afluente para Acoplamento (MWmed)
     - `ENAC`
   * - Energia Armazenada Absoluta Inicial (MWmes)
     - `EARMI`
   * - Energia Armazenada Percentual Inicial (%)
     - `EARPI`
   * - Energia Armazenada Absoluta Final (MWmes)
     - `EARMF`
   * - Energia Armazenada Percentual Final (%)
     - `EARPF`
   * - Energia Vertida (MWmed)
     - `EVER`
   * - Energia Vertida Turbinável (MWmed)
     - `EVERT`
   * - Energia Vertida Não-Turbinável (MWmed)
     - `EVERNT`
   * - Geração Hidráulica (MWmed)
     - `GHID`
   * - Geração Térmica (MWmed)
     - `GTER`
   * - Geração de Usinas Não Simuladas (MWmed)
     - `GUNS`
   * - Geração de Usinas Não Simuladas Disponível (MWmed)
     - `GUND`
   * - Corte de Geração de Usinas Não Simuladas (MWmed)
     - `CUNS`
   * - Intercâmbio (MWmed)
     - `INT`
   * - Intercâmbio Líquido (MWmed)
     - `INTL`
   * - Mercado de Energia (MWmed)
     - `MER`
   * - Mercado de Energia Líquido (MWmes)
     - `MERL`
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
   * - Vazão Evaporada (m3/s)
     - `QEVP`
   * - Volume Armazenado Absoluto Inicial (hm3)
     - `VARMI`
   * - Volume Armazenado Percentual Inicial (%)
     - `VARPI`
   * - Volume Armazenado Absoluto Final (hm3)
     - `VARMF`
   * - Volume Armazenado Percentual Final (%)
     - `VARPF`


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
   * - Par de Submercados
     - `SBP`



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
   * - `COP`
     - `SIN`
   * - `CFU`
     - `SIN`
   * - `CMO`
     - `SBM`
   * - `CTER`
     - `SIN`, `UTE`
   * - `DEF`
     - `SIN`, `SBM`
   * - `ENAA`
     - `SIN`, `SBM`, `REE`
   * - `ENAC`
     - `SIN`, `SBM`, `REE`
   * - `EARMI`
     - `SIN`, `SBM`, `REE`
   * - `EARPI`
     - `SIN`, `SBM`, `REE`
   * - `EARMF`
     - `SIN`, `SBM`, `REE`
   * - `EARPF`
     - `SIN`, `SBM`, `REE`
   * - `EVER`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `EVERT`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `EVERNT`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `GHID`
     - `SIN`, `SBM`, `UHE`
   * - `GTER`
     - `SIN`, `SBM`, `UTE`
   * - `INT`
     - `SBP`
   * - `INTL`
     - `SBP`
   * - `MER`
     - `SIN`, `SBM`
   * - `MERL`
     - `SIN`, `SBM`
   * - `VAGUA`
     - 
   * - `QAFL`
     - `UHE`
   * - `QDES`
     - `UHE`
   * - `QDEF`
     - `UHE`
   * - `QINC`
     - `UHE`
   * - `QRET`
     - `UHE`
   * - `QTUR`
     - `UHE`
   * - `QVER`
     - `UHE`
   * - `QEVP`
     - `UHE`
   * - `VARMI`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARPI`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARMF`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARPF`
     - `UHE`

São exemplos de elementos de dados válidos para as sínteses da operação `EARPF_SBM`, `VARPF_UHE`, `GHID_UHE`, `CMO_SBM`, dentre outras.