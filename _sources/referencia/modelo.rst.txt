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
   * - Custos das Usinas Termoelétricas
     - `CVU`
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
   * - Nome do Modelo Computacional
     - `PROGRAMA`
   * - Versão do Modelo
     - `VERSAO`
   * - Título do Estudo
     - `TITULO`
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

Informações sobre a política operativa construída pelo modelo.

.. list-table:: Dados da Política
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Descrição das variáveis dos cortes de Benders
     - `CORTES_VARIAVEIS`
   * - Coeficientes dos cortes de Benders
     - `CORTES_COEFICIENTES`

Operação
*********

Informações da operação fornecida como saída pelo modelo. Estas informações são formadas a partir de duas especificações:

Variável
=========

A variável informa a grandeza que é modelada e fornecida como saída da operação pelo modelo.

.. list-table:: Variáveis da Operação
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - MNEMÔNICO
   * - Cota de Jusante
     - `HJUS`
   * - Cota de Montante
     - `HMON`
   * - Corte de Geração de Usinas Não Simuladas
     - `CUNS`
   * - Corte de Geração Eólica
     - `VEOL`
   * - Custo da Geração Térmica
     - `CTER`
   * - Custo de Déficit
     - `CDEF`
   * - Custo de Operação
     - `COP`
   * - Custo Futuro
     - `CFU`
   * - Custo Marginal de Operação
     - `CMO`
   * - Custo Total
     - `CTO`
   * - Déficit
     - `DEF`
   * - Energia Armazenada Absoluta Final
     - `EARMF`
   * - Energia Armazenada Absoluta Inicial
     - `EARMI`
   * - Energia Armazenada Percentual Final
     - `EARPF`
   * - Energia Armazenada Percentual Inicial
     - `EARPI`
   * - Energia de Defluência Mínima
     - `EVMIN`
   * - Energia de Enchimento de Volume Morto
     - `EVMOR`
   * - Energia Desviada em Fio d'Água
     - `EDESF`
   * - Energia Desviada em Reservatórios
     - `EDESR`
   * - Energia Evaporada
     - `EEVAP`
   * - Energia Natural Afluente Absoluta
     - `ENAA`
   * - Energia Natural Afluente Absoluta em Fio d'Água
     - `ENAAF`
   * - Energia Natural Afluente Absoluta em Reservatórios
     - `ENAAR`
   * - Energia Natural Afluente para Acoplamento
     - `ENAC`
   * - Energia Vertida
     - `EVER`
   * - Energia Vertida em Fio d'Água
     - `EVERF`
   * - Energia Vertida em Reservatórios
     - `EVERR`
   * - Energia Vertida Não-Turbinável
     - `EVERNT`
   * - Energia Vertida Não-Turbinável em Fio d'Água
     - `EVERFNT`
   * - Energia Vertida Não-Turbinável em Reservatórios
     - `EVERRNT`
   * - Energia Vertida Turbinável
     - `EVERT`
   * - Energia Vertida Turbinável em Fio d'Água
     - `EVERFT`
   * - Energia Vertida Turbinável em Reservatórios
     - `EVERRT`
   * - Excesso de Energia
     - `EXC`
   * - Geração Eólica
     - `GEOL`
   * - Geração Hidráulica
     - `GHID`
   * - Geração Hidráulica em Fio d'Água
     - `GHIDF`
   * - Geração Hidráulica em Reservatórios
     - `GHIDR`
   * - Geração Térmica
     - `GTER`
   * - Geração de Usinas Não Simuladas
     - `GUNS`
   * - Geração de Usinas Não Simuladas Disponível
     - `GUNSD`
   * - Intercâmbio
     - `INT`
   * - Intercâmbio Líquido
     - `INTL`
   * - Mercado de Energia
     - `MER`
   * - Mercado de Energia Líquido
     - `MERL`
   * - Meta de Energia de Defluência Mínima
     - `MEVMIN`
   * - Queda Líquida
     - `HLIQ`
   * - Valor da Água
     - `VAGUA`
   * - Valor da Água Incremental
     - `VAGUAI`
   * - Vazão Afluente
     - `QAFL`
   * - Vazão Defluente
     - `QDEF`
   * - Vazão Desviada
     - `QDES`
   * - Vazão Evaporada
     - `QEVP`
   * - Vazão Incremental
     - `QINC`
   * - Vazão Retirada
     - `QRET`
   * - Vazão Turbinada
     - `QTUR`
   * - Vazão Vertida
     - `QVER`
   * - Velocidade do Vento
     - `VENTO`
   * - Violação de Energia de Defluência Mínima
     - `VEVMIN`
   * - Violação de Evaporação
     - `VEVAP`
   * - Violação de FPHA
     - `VFPHA`
   * - Violação de Geração Hidráulica Mínima
     - `VGHMIN`
   * - Violação Negativa de Evaporação
     - `VNEGEVAP`
   * - Violação Positiva de Evaporação
     - `VPOSEVAP`
   * - Volume Armazenado Absoluto Final
     - `VARMF`
   * - Volume Armazenado Absoluto Inicial
     - `VARMI`
   * - Volume Armazenado na Calha
     - `VCALHA`
   * - Volume Armazenado Percentual Final
     - `VARPF`
   * - Volume Armazenado Percentual Inicial
     - `VARPI`
   * - Volume Afluente
     - `VAFL`
   * - Volume Defluente
     - `VDEF`
   * - Volume Desviado
     - `VDES`
   * - Volume Evaporado
     - `VEVP`
   * - Volume Incremental
     - `VINC`
   * - Volume Retirado
     - `VRET`
   * - Volume Turbinado
     - `VTUR`
   * - Volume Vertido
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
   :widths: 50 10
   :header-rows: 1

   * - VARIÁVEL
     - AGREGAÇÃO ESPACIAL
   * - `HJUS`
     - 
   * - `HMON`
     - 
   * - `VEOL`
     - 
   * - `CUNS`
     - 
   * - `CTER`
     - `SIN`, `UTE`
   * - `CDEF`
     -
   * - `COP`
     - `SIN`
   * - `CFU`
     - `SIN`
   * - `CMO`
     - `SBM`
   * - `CTO`
     - 
   * - `DEF`
     - `SIN`, `SBM`
   * - `EARMF`
     - `SIN`, `SBM`, `REE`
   * - `EARMI`
     - `SIN`, `SBM`, `REE`
   * - `EARPF`
     - `SIN`, `SBM`, `REE`
   * - `EARPI`
     - `SIN`, `SBM`, `REE`
   * - `EVMIN`
     - 
   * - `EVMOR`
     - 
   * - `EDESF`
     - 
   * - `EDESR`
     - 
   * - `EEVAP`
     - 
   * - `ENAA`
     - `SIN`, `SBM`, `REE`
   * - `ENAAF`
     - 
   * - `ENAAR`
     - 
   * - `ENAC`
     - `SIN`, `SBM`, `REE`
   * - `EVER`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `EVERF`
     - 
   * - `EVERR`
     - 
   * - `EVERNT`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `EVERFNT`
     - 
   * - `EVERRNT`
     - 
   * - `EVERT`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `EVERFT`
     - 
   * - `EVERRT`
     - 
   * - `EXC`
     - 
   * - `GEOL`
     - 
   * - `GHID`
     - `SIN`, `SBM`, `UHE`
   * - `GHIDF`
     - 
   * - `GHIDR`
     - 
   * - `GTER`
     - `SIN`, `SBM`, `UTE`
   * - `GUNS`
     - `SIN`, `SBM`
   * - `GUNSD`
     - 
   * - `INT`
     - `SBP`
   * - `INTL`
     - `SBP`
   * - `MER`
     - `SIN`, `SBM`
   * - `MERL`
     - `SIN`, `SBM`
   * - `MEVMIN`
     - 
   * - `HLIQ`
     - 
   * - `VAGUA`
     - 
   * - `VAGUAI`
     - 
   * - `QAFL`
     - `UHE`
   * - `QDEF`
     - `UHE`
   * - `QDES`
     - `UHE`
   * - `QEVP`
     - `UHE`
   * - `QINC`
     - `UHE`
   * - `QRET`
     - `UHE`
   * - `QTUR`
     - `UHE`
   * - `QVER`
     - `UHE`
   * - `VENTO`
     - 
   * - `VEVMIN`
     - 
   * - `VEVAP`
     - 
   * - `VFPHA`
     - 
   * - `VGHMIN`
     - 
   * - `VNEGEVAP`
     - 
   * - `VPOSEVAP`
     - 
   * - `VARMF`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VARMI`
     - `SIN`, `SBM`, `REE`, `UHE`
   * - `VCALHA`
     - 
   * - `VARPF`
     - `UHE`
   * - `VARPI`
     - `UHE`
   * - `VAFL`
     - 
   * - `VDEF`
     - 
   * - `VDES`
     - 
   * - `VEVP`
     - 
   * - `VINC`
     - 
   * - `VRET`
     - 
   * - `VTUR`
     - 
   * - `VVER`
     - 

São exemplos de elementos de dados válidos para as sínteses da operação `EARPF_SBM`, `VARPF_UHE`, `GHID_UHE`, `CMO_SBM`, dentre outras.