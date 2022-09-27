from abc import ABC, abstractmethod
from typing import Dict, Type, Optional, Tuple, Callable
import pandas as pd  # type: ignore

from idecomp.decomp.caso import Caso
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.dadgnl import DadGNL
from idecomp.decomp.inviabunic import InviabUnic
from idecomp.decomp.relato import Relato
from idecomp.decomp.relgnl import RelGNL

from sintetizador.utils.log import Log
from sintetizador.model.operation.variable import Variable
from sintetizador.model.operation.spatialresolution import SpatialResolution
from sintetizador.model.operation.temporalresolution import TemporalResolution


class AbstractFilesRepository(ABC):
    @property
    @abstractmethod
    def caso(self) -> Caso:
        raise NotImplementedError

    @property
    @abstractmethod
    def arquivos(self) -> Arquivos:
        raise NotImplementedError

    @abstractmethod
    def get_dadger(self) -> Dadger:
        raise NotImplementedError

    @abstractmethod
    def get_dadgnl(self) -> DadGNL:
        raise NotImplementedError

    @abstractmethod
    def get_inviab_unic(self) -> InviabUnic:
        raise NotImplementedError

    @abstractmethod
    def get_relato(self) -> Relato:
        raise NotImplementedError

    @abstractmethod
    def get_relato2(self) -> Relato:
        raise NotImplementedError

    @abstractmethod
    def get_relgnl(self) -> RelGNL:
        raise NotImplementedError


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        self.__caso = Caso.le_arquivo(str(self.__tmppath))
        self.__arquivos: Optional[Arquivos] = None
        self.__dadger: Optional[Dadger] = None
        self.__dadgnl: Optional[DadGNL] = None
        self.__relato: Optional[Relato] = None
        self.__relato2: Optional[Relato] = None
        self.__inviabunic: Optional[InviabUnic] = None
        self.__relgnl: Optional[RelGNL] = None

    def __extrai_patamares_df(
        self, df: pd.DataFrame, patamares: list = None
    ) -> pd.DataFrame:
        if patamares is None:
            patamares = [
                str(i)
                for i in range(1, self.get_patamar().numero_patamares + 1)
            ]
        return df.loc[df["Patamar"].isin(patamares), :]

    @property
    def caso(self) -> Caso:
        return self.__caso

    @property
    def arquivos(self) -> Arquivos:
        if self.__arquivos is None:
            self.__arquivos = Arquivos.le_arquivo(
                self.__tmppath, self.__caso.arquivos
            )
        return self.__arquivos

    def get_dadger(self) -> Dadger:
        if self.__dadger is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.dadger}")
            self.__dadger = Dadger.le_arquivo(
                self.__tmppath, self.arquivos.dadger
            )
        return self.__dadger

    def get_dadgnl(self) -> DadGNL:
        if self.__dadgnl is None:
            Log.log().info(f"Lendo arquivo {self.arquivos.dadgnl}")
            self.__dadgnl = DadGNL.le_arquivo(
                self.__tmppath, self.arquivos.dadgnl
            )
        return self.__dadgnl

    def get_relato(self) -> Relato:
        if self.__relato is None:
            Log.log().info(f"Lendo arquivo relato.{self.caso.arquivos}")
            self.__relato = Relato.le_arquivo(
                self.__tmppath, f"relato.{self.caso.arquivos}"
            )
        return self.__relato

    def get_relato2(self) -> Relato:
        if self.__relato2 is None:
            Log.log().info(f"Lendo arquivo relato2.{self.caso.arquivos}")
            self.__relato2 = Relato.le_arquivo(
                self.__tmppath, f"relato2.{self.caso.arquivos}"
            )
        return self.__relato2

    def get_inviabunic(self) -> InviabUnic:
        if self.__inviabunic is None:
            Log.log().info(f"Lendo arquivo inviab_unic.{self.caso.arquivos}")
            self.__inviabunic = Relato.le_arquivo(
                self.__tmppath, f"inviab_unic.{self.caso.arquivos}"
            )
        return self.__inviabunic

    def get_relgnl(self) -> RelGNL:
        if self.__relgnl is None:
            Log.log().info(f"Lendo arquivo relgnl.{self.caso.arquivos}")
            self.__relgnl = RelGNL.le_arquivo(
                self.__tmppath, f"relgnl.{self.caso.arquivos}"
            )
        return self.__relgnl


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind)(*args, **kwargs)
