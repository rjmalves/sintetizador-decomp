from abc import ABC, abstractmethod
from typing import Dict, Type, Optional
import pathlib
import asyncio
from os.path import join

from idecomp.decomp.caso import Caso
from idecomp.decomp.arquivos import Arquivos
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.dadgnl import DadGNL
from idecomp.decomp.inviabunic import InviabUnic
from idecomp.decomp.relato import Relato
from idecomp.decomp.decomptim import DecompTim
from idecomp.decomp.relgnl import RelGNL
from idecomp.decomp.hidr import Hidr
from idecomp.decomp.dec_oper_usih import DecOperUsih
from idecomp.decomp.dec_oper_usit import DecOperUsit
from idecomp.decomp.dec_oper_ree import DecOperRee
from idecomp.decomp.dec_oper_interc import DecOperInterc

from sintetizador.model.settings import Settings
from sintetizador.utils.encoding import converte_codificacao
from sintetizador.utils.log import Log


import platform

if platform.system() == "Windows":
    Dadger.ENCODING = "iso-8859-1"


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
    def get_dadgnl(self) -> Optional[DadGNL]:
        raise NotImplementedError

    @abstractmethod
    def get_inviabunic(self) -> Optional[InviabUnic]:
        raise NotImplementedError

    @abstractmethod
    def get_relato(self) -> Relato:
        raise NotImplementedError

    @abstractmethod
    def get_decomptim(self) -> DecompTim:
        raise NotImplementedError

    @abstractmethod
    def get_relato2(self) -> Optional[Relato]:
        raise NotImplementedError

    @abstractmethod
    def get_relgnl(self) -> Optional[RelGNL]:
        raise NotImplementedError

    @abstractmethod
    def get_hidr(self) -> Hidr:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_usih(self) -> DecOperUsih:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_usit(self) -> DecOperUsit:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_ree(self) -> DecOperRee:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_interc(self) -> DecOperInterc:
        raise NotImplementedError


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        try:
            self.__caso = Caso.read(join(str(self.__tmppath), "caso.dat"))
        except FileNotFoundError as e:
            logger = Log.log()
            if logger is not None:
                logger.error("Não foi encontrado o arquivo caso.dat")
            raise e
        self.__arquivos: Optional[Arquivos] = None
        self.__dadger: Optional[Dadger] = None
        self.__read_dadger = False
        self.__dadgnl: Optional[DadGNL] = None
        self.__read_dadgnl = False
        self.__relato: Optional[Relato] = None
        self.__read_relato = False
        self.__relato2: Optional[Relato] = None
        self.__read_relato2 = False
        self.__decomptim: Optional[DecompTim] = None
        self.__read_decomptim = False
        self.__inviabunic: Optional[InviabUnic] = None
        self.__read_inviabunic = False
        self.__relgnl: Optional[RelGNL] = None
        self.__read_relgnl = False
        self.__hidr: Optional[Hidr] = None
        self.__read_hidr = False
        self.__dec_oper_usih: Optional[DecOperUsih] = None
        self.__read_dec_oper_usih = False
        self.__dec_oper_usit: Optional[DecOperUsit] = None
        self.__read_dec_oper_usit = False
        self.__dec_oper_ree: Optional[DecOperRee] = None
        self.__read_dec_oper_ree = False
        self.__dec_oper_interc: Optional[DecOperInterc] = None
        self.__read_dec_oper_interc = False

    @property
    def caso(self) -> Caso:
        return self.__caso

    @property
    def arquivos(self) -> Arquivos:
        if self.__arquivos is None:
            logger = Log.log()
            try:
                self.__arquivos = Arquivos.read(
                    join(self.__tmppath, self.__caso.arquivos)
                )
            except FileNotFoundError as e:
                if logger is not None:
                    logger.error(
                        f"Não foi encontrado o arquivo {self.__caso.arquivos}"
                    )
                raise e
        return self.__arquivos

    def get_dadger(self) -> Dadger:
        if self.__read_dadger is False:
            self.__read_dadger = True
            logger = Log.log()
            try:
                caminho = pathlib.Path(self.__tmppath).joinpath(
                    self.arquivos.dadger
                )
                script = pathlib.Path(Settings().installdir).joinpath(
                    Settings().encoding_script
                )
                asyncio.run(converte_codificacao(caminho, script))

                if logger is not None:
                    logger.info(f"Lendo arquivo {self.arquivos.dadger}")
                self.__dadger = Dadger.read(
                    join(self.__tmppath, self.arquivos.dadger)
                )
            except Exception as e:
                if logger is not None:
                    logger.error(
                        f"Erro na leitura do {self.arquivos.dadger}: {e}"
                    )
                raise e
        return self.__dadger

    def get_dadgnl(self) -> Optional[DadGNL]:
        if self.__read_dadgnl is False:
            self.__read_dadgnl = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info(f"Lendo arquivo {self.arquivos.dadgnl}")
                self.__dadgnl = DadGNL.read(
                    join(self.__tmppath, self.arquivos.dadgnl)
                )
            except FileNotFoundError:
                if logger is not None:
                    logger.info(
                        f"Não encontrado arquivo {self.arquivos.dadgnl}"
                    )
                return None
            except Exception as e:
                if logger is not None:
                    logger.info(
                        f"Erro na leitura do {self.arquivos.dadgnl}: {e}"
                    )
                return None
        return self.__dadgnl

    def get_relato(self) -> Relato:
        if self.__read_relato is False:
            self.__read_relato = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info(f"Lendo arquivo relato.{self.caso.arquivos}")
                self.__relato = Relato.read(
                    join(self.__tmppath, f"relato.{self.caso.arquivos}")
                )
            except Exception as e:
                if logger is not None:
                    logger.error(
                        f"Erro na leitura do relato.{self.caso.arquivos}: {e}"
                    )
                raise e
        return self.__relato

    def get_relato2(self) -> Optional[Relato]:
        if self.__read_relato2 is False:
            self.__read_relato2 = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info(f"Lendo arquivo relato2.{self.caso.arquivos}")
                self.__relato2 = Relato.read(
                    join(self.__tmppath, f"relato2.{self.caso.arquivos}")
                )
            except FileNotFoundError:
                if logger is not None:
                    logger.info(
                        f"Não encontrado arquivo relato2.{self.caso.arquivos}"
                    )
                return None
            except Exception as e:
                if logger is not None:
                    logger.info(
                        f"Erro na leitura do relato2.{self.caso.arquivos}: {e}"
                    )
                return None
        return self.__relato2

    def get_decomptim(self) -> DecompTim:
        if self.__read_decomptim is False:
            self.__read_decomptim = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo decomp.tim")
                self.__decomptim = DecompTim.read(
                    join(self.__tmppath, "decomp.tim")
                )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do decomp.tim: {e}")
                raise e
        return self.__decomptim

    def get_inviabunic(self) -> Optional[InviabUnic]:
        if self.__read_inviabunic is False:
            self.__read_inviabunic = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info(
                        f"Lendo arquivo inviab_unic.{self.caso.arquivos}"
                    )
                self.__inviabunic = InviabUnic.read(
                    join(self.__tmppath, f"inviab_unic.{self.caso.arquivos}")
                )
            except FileNotFoundError:
                if logger is not None:
                    logger.info(
                        f"Não encontrado arquivo inviab_unic.{self.caso.arquivos}"
                    )
                return None
            except Exception as e:
                if logger is not None:
                    logger.info(
                        f"Erro na leitura do inviab_unic.{self.caso.arquivos}: {e}"
                    )
                return None
        return self.__inviabunic

    def get_relgnl(self) -> Optional[RelGNL]:
        if self.__read_relgnl is False:
            self.__read_relgnl = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info(f"Lendo arquivo relgnl.{self.caso.arquivos}")
                self.__relgnl = RelGNL.read(
                    join(self.__tmppath, f"relgnl.{self.caso.arquivos}")
                )
            except FileNotFoundError:
                if logger is not None:
                    logger.info(
                        f"Não encontrado arquivo relgnl.{self.caso.arquivos}"
                    )
                return None
            except Exception as e:
                if logger is not None:
                    logger.info(
                        f"Erro na leitura do relgnl.{self.caso.arquivos}: {e}"
                    )
                return None
        return self.__relgnl

    def get_hidr(self) -> Hidr:
        if self.__read_hidr is False:
            self.__read_hidr = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info(f"Lendo arquivo {self.arquivos.hidr}")
                self.__hidr = Hidr.read(
                    join(self.__tmppath, self.arquivos.hidr)
                )
            except Exception as e:
                if logger is not None:
                    logger.error(
                        f"Erro na leitura do {self.arquivos.hidr}: {e}"
                    )
                raise e
        return self.__hidr

    def get_dec_oper_usih(self) -> DecOperUsih:
        if self.__read_dec_oper_usih is False:
            self.__read_dec_oper_usih = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_oper_usih.csv")
                self.__dec_oper_usih = DecOperUsih.read(
                    join(self.__tmppath, "dec_oper_usih.csv")
                )
                if self.__dec_oper_usih.versao <= "31.0.2":
                    DecOperUsih.set_version("31.0.2")
                    self.__dec_oper_usih = DecOperUsih.read(
                        join(self.__tmppath, "dec_oper_usih.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dec_oper_usih.csv: {e}")
                raise e
        return self.__dec_oper_usih

    def get_dec_oper_usit(self) -> DecOperUsit:
        if self.__read_dec_oper_usit is False:
            self.__read_dec_oper_usit = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_oper_usit.csv")
                self.__dec_oper_usit = DecOperUsit.read(
                    join(self.__tmppath, "dec_oper_usit.csv")
                )
                if self.__dec_oper_usit.versao <= "31.0.2":
                    DecOperUsit.set_version("31.0.2")
                    self.__dec_oper_usit = DecOperUsit.read(
                        join(self.__tmppath, "dec_oper_usit.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dec_oper_usit.csv: {e}")
                raise e
        return self.__dec_oper_usit

    def get_dec_oper_ree(self) -> DecOperRee:
        if self.__read_dec_oper_ree is False:
            self.__read_dec_oper_ree = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_oper_ree.csv")
                self.__dec_oper_ree = DecOperRee.read(
                    join(self.__tmppath, "dec_oper_ree.csv")
                )
                if self.__dec_oper_ree.versao <= "31.0.2":
                    DecOperRee.set_version("31.0.2")
                    self.__dec_oper_ree = DecOperRee.read(
                        join(self.__tmppath, "dec_oper_ree.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dec_oper_ree.csv: {e}")
                raise e
        return self.__dec_oper_ree

    def get_dec_oper_interc(self) -> DecOperRee:
        if self.__read_dec_oper_interc is False:
            self.__read_dec_oper_interc = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_oper_interc.csv")
                self.__dec_oper_interc = DecOperInterc.read(
                    join(self.__tmppath, "dec_oper_interc.csv")
                )
                if self.__dec_oper_interc.versao <= "31.0.2":
                    DecOperInterc.set_version("31.0.2")
                    self.__dec_oper_interc = DecOperInterc.read(
                        join(self.__tmppath, "dec_oper_interc.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(
                        f"Erro na leitura do dec_oper_interc.csv: {e}"
                    )
                raise e
        return self.__dec_oper_interc


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)
