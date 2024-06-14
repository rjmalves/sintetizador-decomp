import pandas as pd  # type: ignore
from typing import Callable
from idecomp.decomp.hidr import Hidr
from dataclasses import dataclass
from enum import Enum


class InfeasibilityType(Enum):
    RE = "RE"
    RHA = "RHA"
    TI = "TI"
    VERT = "VERT"
    RHV = "RHV"
    RHE = "RHE"
    RHQ = "RHQ"
    EV = "EV"
    DEFMIN = "DEFMIN"
    FP = "FP"
    DEFICIT = "DEFICIT"

    @property
    def message_pattern(self) -> str:
        pattern_map = {
            self.RE: "RESTRICAO ELETRICA",
            self.RHA: "RHA",
            self.TI: "IRRIGACAO",
            self.VERT: "VERT. PERIODO",
            self.RHV: "RHV",
            self.RHQ: "RHQ",
            self.RHE: "RHE",
            self.EV: "EVAPORACAO",
            self.DEFMIN: "DEF. MINIMA",
            self.FP: "FUNCAO DE PRODUCAO",
            self.DEFICIT: "DEFICIT",
        }

        return pattern_map[self]


@dataclass
class Infeasibility:
    type: str
    iteration: int
    stage: int
    scenario: int
    violation: float
    unit: str
    constraint_code: int | None = None
    block: int | None = None
    bound: str | None = None
    submarket: str | None = None

    @classmethod
    def _build_RE(
        cls,
        iteration: int,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str) -> tuple:
            r = InfeasibilityType.RE.message_pattern
            code = int(constraint_message.split(r)[1].split("PATAMAR")[0])
            block = int(constraint_message.split("PATAMAR")[1].split("(")[0])
            bound = constraint_message.split("(")[1].split(")")[0]
            return (code, block, bound)

        code, block, bound = _process_message(constraint_message)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.RE.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            block=block,
            bound=bound,
        )
        return infeasibility_data

    @classmethod
    def _build_RHA(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str) -> tuple:
            code = int(constraint_message.split("RHA")[1].split(":")[0])
            bound = constraint_message.split("(")[1].split(")")[0]
            return (code, bound)

        code, bound = _process_message(constraint_message)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.RHA.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            bound=bound,
        )
        return infeasibility_data

    @classmethod
    def _build_RHQ(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str) -> tuple:
            code = int(constraint_message.split("RHQ")[1].split(":")[0])
            block = int(constraint_message.split("PATAMAR")[1].split("(")[0])
            bound = constraint_message.split("(")[1].split(")")[0]
            return (code, block, bound)

        code, block, bound = _process_message(constraint_message)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.RHQ.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            block=block,
            bound=bound,
        )
        return infeasibility_data

    @classmethod
    def _build_TI(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str, hidr: Hidr) -> int:
            name = constraint_message.split("IRRIGACAO, USINA")[1].strip()
            code = int(
                list(
                    hidr.cadastro.loc[
                        hidr.cadastro["nome_usina"] == name, :
                    ].index
                )[0]
            )
            return code

        code = _process_message(constraint_message, hidr)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.TI.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
        )
        return infeasibility_data

    @classmethod
    def _build_VERT(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str, hidr: Hidr) -> tuple:
            block = int(
                constraint_message.split("USINA")[0].split("PAT. ")[1].strip()
            )
            name = constraint_message.split("USINA")[1].strip()
            code = int(
                list(
                    hidr.cadastro.loc[
                        hidr.cadastro["nome_usina"] == name, :
                    ].index
                )[0]
            )
            return (code, block)

        code, block = _process_message(constraint_message, hidr)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.VERT.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            block=block,
        )
        return infeasibility_data

    @classmethod
    def _build_RHV(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str) -> tuple:
            code = int(constraint_message.split("RHV")[1].split(":")[0])
            bound = constraint_message.split("(")[1].split(")")[0]
            return (code, bound)

        code, bound = _process_message(constraint_message)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.RHV.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            bound=bound,
        )
        return infeasibility_data

    @classmethod
    def _build_RHE(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str) -> tuple:
            s_rhe = "RESTRICAO RHE - NUMERO"
            s_per = "PERIODO"
            code = int(constraint_message.split(s_rhe)[1].split(",")[0])
            stage = int(constraint_message.split(s_per)[1].split("(")[0])
            bound = constraint_message.split("(")[1].split(")")[0]
            return (code, stage, bound)

        code, stage, bound = _process_message(constraint_message)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.RHE.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            bound=bound,
        )
        return infeasibility_data

    @classmethod
    def _build_EV(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str, hidr: Hidr) -> int:
            name = constraint_message.split("EVAPORACAO, USINA")[1].strip()
            code = int(
                list(
                    hidr.cadastro.loc[
                        hidr.cadastro["nome_usina"] == name, :
                    ].index
                )[0]
            )
            return code

        code = _process_message(constraint_message, hidr)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.EV.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
        )
        return infeasibility_data

    @classmethod
    def _build_DEFMIN(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str, hidr: Hidr) -> tuple:
            block_string = "PATAMAR"
            hydro_string = "USINA"
            block = int(
                constraint_message.split(block_string)[1]
                .split(hydro_string)[0]
                .strip()
            )
            name = constraint_message.split(hydro_string)[1].strip()
            code = int(
                list(
                    hidr.cadastro.loc[
                        hidr.cadastro["nome_usina"] == name, :
                    ].index
                )[0]
            )
            return (code, block)

        code, block = _process_message(constraint_message, hidr)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.DEFMIN.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            block=block,
        )
        return infeasibility_data

    @classmethod
    def _build_FP(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(constraint_message: str, hidr: Hidr) -> tuple:
            block_string = "PATAMAR"
            hydro_string = "USINA"
            block = int(constraint_message.split(block_string)[1])
            name = (
                constraint_message.split(hydro_string)[1].split(",")[0].strip()
            )
            code = int(
                list(
                    hidr.cadastro.loc[
                        hidr.cadastro["nome_usina"] == name, :
                    ].index
                )[0]
            )
            return (code, block)

        code, block = _process_message(constraint_message, hidr)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.FP.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=code,
            violation=violation,
            unit=unit,
            block=block,
        )
        return infeasibility_data

    @classmethod
    def _build_DEFICIT(
        cls,
        iteration,
        stage,
        scenario,
        constraint_message,
        violation,
        unit,
        hidr,
    ) -> "Infeasibility":
        def _process_message(
            constraint_message: str,
        ) -> tuple:
            submarket = (
                constraint_message.split("SUBSISTEMA ")[1].split(",")[0].strip()
            )
            block = int(constraint_message.split("PATAMAR")[1].strip())

            return (submarket, block)

        submarket, block = _process_message(constraint_message)

        infeasibility_data = Infeasibility(
            type=InfeasibilityType.DEFICIT.value,
            iteration=iteration,
            stage=stage,
            scenario=scenario,
            constraint_code=None,
            violation=violation,
            unit=unit,
            block=block,
            submarket=submarket,
        )
        return infeasibility_data

    @classmethod
    def factory(
        cls, inviab_unic_line: pd.Series, hidr: Hidr
    ) -> "Infeasibility":
        iteration = int(inviab_unic_line["iteracao"])
        stage = int(inviab_unic_line["estagio"])
        scenario = int(inviab_unic_line["cenario"])
        constraint_message = str(inviab_unic_line["restricao"])
        violation = float(inviab_unic_line["violacao"])
        unit = str(inviab_unic_line["unidade"])
        inseasibility_map: dict[str, Callable] = {
            InfeasibilityType.RE.message_pattern: cls._build_RE,
            InfeasibilityType.RHA.message_pattern: cls._build_RHA,
            InfeasibilityType.RHQ.message_pattern: cls._build_RHQ,
            InfeasibilityType.TI.message_pattern: cls._build_TI,
            InfeasibilityType.VERT.message_pattern: cls._build_VERT,
            InfeasibilityType.RHV.message_pattern: cls._build_RHV,
            InfeasibilityType.RHE.message_pattern: cls._build_RHE,
            InfeasibilityType.EV.message_pattern: cls._build_EV,
            InfeasibilityType.DEFMIN.message_pattern: cls._build_DEFMIN,
            InfeasibilityType.FP.message_pattern: cls._build_FP,
            InfeasibilityType.DEFICIT.message_pattern: cls._build_DEFICIT,
        }
        infeasibilities_types = list(inseasibility_map.keys())
        type = list(
            filter(lambda s: s in constraint_message, infeasibilities_types)
        )
        if len(type) == 0:
            raise TypeError(f"Restrição {constraint_message} não suportada")
        elif len(type) > 1:
            raise TypeError(f"Mensagem {constraint_message} ambígua: {type}")
        else:
            return inseasibility_map[type[0]](
                iteration,
                stage,
                scenario,
                constraint_message,
                violation,
                unit,
                hidr,
            )
