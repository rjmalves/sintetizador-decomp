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
from idecomp.decomp.dec_oper_gnl import DecOperGnl
from idecomp.decomp.dec_oper_ree import DecOperRee
from idecomp.decomp.dec_oper_sist import DecOperSist
from idecomp.decomp.dec_oper_interc import DecOperInterc
from idecomp.decomp.dec_eco_discr import DecEcoDiscr

from sintetizador.model.settings import Settings
from sintetizador.utils.encoding import converte_codificacao
from sintetizador.utils.log import Log


import platform

if platform.system() == "Windows":
    Dadger.ENCODING = "iso-8859-1"


class AbstractFilesRepository(ABC):
    @property
    @abstractmethod
    def extensao(self) -> str:
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
    def get_inviabunic(self) -> InviabUnic:
        raise NotImplementedError

    @abstractmethod
    def get_relato(self) -> Relato:
        raise NotImplementedError

    @abstractmethod
    def get_decomptim(self) -> DecompTim:
        raise NotImplementedError

    @abstractmethod
    def get_relato2(self) -> Relato:
        raise NotImplementedError

    @abstractmethod
    def get_relgnl(self) -> RelGNL:
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
    def get_dec_oper_gnl(self) -> DecOperGnl:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_ree(self) -> DecOperRee:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_sist(self) -> DecOperSist:
        raise NotImplementedError

    @abstractmethod
    def get_dec_oper_interc(self) -> DecOperInterc:
        raise NotImplementedError

    @abstractmethod
    def get_dec_eco_discr(self) -> DecEcoDiscr:
        raise NotImplementedError


class RawFilesRepository(AbstractFilesRepository):
    def __init__(self, tmppath: str):
        self.__tmppath = tmppath
        try:
            arq_caso = Caso.read(join(str(self.__tmppath), "caso.dat"))
            extensao = arq_caso.arquivos
            if extensao is None:
                raise FileNotFoundError()
            self.__extensao = extensao
        except FileNotFoundError as e:
            logger = Log.log()
            if logger is not None:
                logger.error("Erro na leitura do arquivo arquivo caso.dat")
            raise e
        self.__arquivos: Optional[Arquivos] = None
        self.__read_dadger = False
        self.__read_dadgnl = False
        self.__read_relato = False
        self.__read_relato2 = False
        self.__read_decomptim = False
        self.__read_inviabunic = False
        self.__read_relgnl = False
        self.__read_hidr = False
        self.__read_dec_oper_usih = False
        self.__read_dec_oper_usit = False
        self.__read_dec_oper_gnl = False
        self.__read_dec_oper_ree = False
        self.__read_dec_oper_sist = False
        self.__read_dec_oper_interc = False
        self.__read_dec_eco_discr = False

    @property
    def extensao(self) -> str:
        return self.__extensao

    @property
    def arquivos(self) -> Arquivos:
        if self.__arquivos is None:
            logger = Log.log()
            try:
                self.__arquivos = Arquivos.read(
                    join(self.__tmppath, self.extensao)
                )
            except FileNotFoundError as e:
                if logger is not None:
                    logger.error(
                        f"Não foi encontrado o arquivo {self.extensao}"
                    )
                raise e
        return self.__arquivos

    def get_dadger(self) -> Dadger:
        if self.__read_dadger is False:
            self.__read_dadger = True
            logger = Log.log()
            try:
                arq_dadger = self.arquivos.dadger
                if arq_dadger is None:
                    raise FileNotFoundError()
                caminho = str(
                    pathlib.Path(self.__tmppath).joinpath(arq_dadger)
                )
                script = str(
                    pathlib.Path(Settings().installdir).joinpath(
                        Settings().encoding_script
                    )
                )
                asyncio.run(converte_codificacao(caminho, script))

                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_dadger}")

                self.__dadger = Dadger.read(join(self.__tmppath, arq_dadger))
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dadger: {e}")
                raise e
        return self.__dadger

    def get_dadgnl(self) -> DadGNL:
        if self.__read_dadgnl is False:
            self.__read_dadgnl = True
            logger = Log.log()
            try:
                arq_dadgnl = self.arquivos.dadgnl
                if arq_dadgnl is None:
                    raise FileNotFoundError()
                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_dadgnl}")
                self.__dadgnl = DadGNL.read(join(self.__tmppath, arq_dadgnl))
            except Exception as e:
                if logger is not None:
                    logger.info(f"Erro na leitura do dadgnl: {e}")
                raise e
        return self.__dadgnl

    def get_relato(self) -> Relato:
        if self.__read_relato is False:
            self.__read_relato = True
            logger = Log.log()
            try:
                arq_relato = f"relato.{self.extensao}"
                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_relato}")
                self.__relato = Relato.read(join(self.__tmppath, arq_relato))
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do {arq_relato}: {e}")
                raise e
        return self.__relato

    def get_relato2(self) -> Relato:
        if self.__read_relato2 is False:
            self.__read_relato2 = True
            logger = Log.log()
            try:
                arq_relato2 = f"relato2.{self.extensao}"
                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_relato2}")
                self.__relato2 = Relato.read(join(self.__tmppath, arq_relato2))
            except FileNotFoundError:
                if logger is not None:
                    logger.info(f"Não encontrado arquivo {arq_relato2}")
                raise RuntimeError()
            except Exception as e:
                if logger is not None:
                    logger.info(f"Erro na leitura do {arq_relato2}: {e}")
                raise e
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

    def get_inviabunic(self) -> InviabUnic:
        if self.__read_inviabunic is False:
            self.__read_inviabunic = True
            logger = Log.log()
            try:
                arq_inviabunic = f"inviab_unic.{self.extensao}"
                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_inviabunic}")
                self.__inviabunic = InviabUnic.read(
                    join(self.__tmppath, arq_inviabunic)
                )
            except FileNotFoundError:
                if logger is not None:
                    logger.info(f"Não encontrado arquivo {arq_inviabunic}")
                raise RuntimeError()
            except Exception as e:
                if logger is not None:
                    logger.info(f"Erro na leitura do {arq_inviabunic}: {e}")
                raise e
        return self.__inviabunic

    def get_relgnl(self) -> RelGNL:
        if self.__read_relgnl is False:
            self.__read_relgnl = True
            logger = Log.log()
            try:
                arq_relgnl = f"relgnl.{self.extensao}"
                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_relgnl}")
                self.__relgnl = RelGNL.read(join(self.__tmppath, arq_relgnl))
            except FileNotFoundError:
                if logger is not None:
                    logger.info(f"Não encontrado arquivo {arq_relgnl}")
                raise RuntimeError()
            except Exception as e:
                if logger is not None:
                    logger.info(f"Erro na leitura do {arq_relgnl}: {e}")
                raise e
        return self.__relgnl

    def get_hidr(self) -> Hidr:
        if self.__read_hidr is False:
            self.__read_hidr = True
            logger = Log.log()
            try:
                arq_hidr = self.arquivos.hidr
                if arq_hidr is None:
                    raise FileNotFoundError()
                if logger is not None:
                    logger.info(f"Lendo arquivo {arq_hidr}")
                self.__hidr = Hidr.read(join(self.__tmppath, arq_hidr))
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do {arq_hidr}: {e}")
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

    def get_dec_oper_gnl(self) -> DecOperGnl:
        if self.__read_dec_oper_gnl is False:
            self.__read_dec_oper_gnl = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_oper_gnl.csv")
                self.__dec_oper_gnl = DecOperGnl.read(
                    join(self.__tmppath, "dec_oper_gnl.csv")
                )
                if self.__dec_oper_gnl.versao <= "31.0.2":
                    DecOperGnl.set_version("31.0.2")
                    self.__dec_oper_gnl = DecOperGnl.read(
                        join(self.__tmppath, "dec_oper_gnl.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dec_oper_gnl.csv: {e}")
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

    def get_dec_oper_sist(self) -> DecOperSist:
        if self.__read_dec_oper_sist is False:
            self.__read_dec_oper_sist = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_oper_sist.csv")
                self.__dec_oper_sist = DecOperSist.read(
                    join(self.__tmppath, "dec_oper_sist.csv")
                )
                if self.__dec_oper_sist.versao <= "31.0.2":
                    DecOperSist.set_version("31.0.2")
                    self.__dec_oper_sist = DecOperSist.read(
                        join(self.__tmppath, "dec_oper_sist.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dec_oper_sist.csv: {e}")
                raise e
        return self.__dec_oper_sist

    def get_dec_oper_interc(self) -> DecOperInterc:
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

    def get_dec_eco_discr(self) -> DecEcoDiscr:
        if self.__read_dec_eco_discr is False:
            self.__read_dec_eco_discr = True
            logger = Log.log()
            try:
                if logger is not None:
                    logger.info("Lendo arquivo dec_eco_discr.csv")
                self.__dec_eco_discr = DecEcoDiscr.read(
                    join(self.__tmppath, "dec_eco_discr.csv")
                )
                if self.__dec_eco_discr.versao <= "31.0.2":
                    DecEcoDiscr.set_version("31.0.2")
                    self.__dec_eco_discr = DecEcoDiscr.read(
                        join(self.__tmppath, "dec_eco_discr.csv")
                    )
            except Exception as e:
                if logger is not None:
                    logger.error(f"Erro na leitura do dec_eco_discr.csv: {e}")
                raise e
        return self.__dec_eco_discr


def factory(kind: str, *args, **kwargs) -> AbstractFilesRepository:
    mapping: Dict[str, Type[AbstractFilesRepository]] = {
        "FS": RawFilesRepository
    }
    return mapping.get(kind, RawFilesRepository)(*args, **kwargs)
