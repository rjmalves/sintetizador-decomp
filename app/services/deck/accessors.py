from __future__ import annotations

from typing import Optional

from idecomp.decomp import (  # type: ignore[attr-defined]
    Dadger,
    Decomptim,
    Hidr,
    InviabUnic,
    Relato,
    Vazoes,
)
from idecomp.decomp.avl_turb_max import AvlTurbMax
from idecomp.decomp.dec_eco_discr import DecEcoDiscr
from idecomp.decomp.dec_fcf_cortes import DecFcfCortes
from idecomp.decomp.dec_oper_gnl import DecOperGnl
from idecomp.decomp.dec_oper_interc import DecOperInterc
from idecomp.decomp.dec_oper_ree import DecOperRee
from idecomp.decomp.dec_oper_sist import DecOperSist
from idecomp.decomp.dec_oper_usih import DecOperUsih
from idecomp.decomp.dec_oper_usit import DecOperUsit

from app.services.unitofwork import AbstractUnitOfWork


def get_dadger(uow: AbstractUnitOfWork) -> Dadger:
    with uow:
        dadger = uow.files.get_dadger()
        return dadger


def get_relato(uow: AbstractUnitOfWork) -> Relato:
    with uow:
        relato = uow.files.get_relato()
        return relato


def get_relato2(uow: AbstractUnitOfWork) -> Relato:
    with uow:
        relato = uow.files.get_relato2()
        return relato


def get_inviabunic(uow: AbstractUnitOfWork) -> InviabUnic:
    with uow:
        inviabunic = uow.files.get_inviabunic()
        return inviabunic


def get_decomptim(uow: AbstractUnitOfWork) -> Decomptim:
    with uow:
        decomptim = uow.files.get_decomptim()
        return decomptim


def get_vazoes(uow: AbstractUnitOfWork) -> Vazoes:
    with uow:
        vazoes = uow.files.get_vazoes()
        return vazoes


def get_hidr(uow: AbstractUnitOfWork) -> Hidr:
    with uow:
        hidr = uow.files.get_hidr()
        return hidr


def get_dec_eco_discr(uow: AbstractUnitOfWork) -> DecEcoDiscr:
    with uow:
        dec = uow.files.get_dec_eco_discr()
        return dec


def get_dec_oper_sist(uow: AbstractUnitOfWork) -> DecOperSist:
    with uow:
        dec = uow.files.get_dec_oper_sist()
        return dec


def get_dec_oper_ree(uow: AbstractUnitOfWork) -> DecOperRee:
    with uow:
        dec = uow.files.get_dec_oper_ree()
        return dec


def get_dec_oper_usih(uow: AbstractUnitOfWork) -> DecOperUsih:
    with uow:
        dec = uow.files.get_dec_oper_usih()
        return dec


def get_dec_oper_usit(uow: AbstractUnitOfWork) -> DecOperUsit:
    with uow:
        dec = uow.files.get_dec_oper_usit()
        return dec


def get_dec_oper_gnl(uow: AbstractUnitOfWork) -> DecOperGnl:
    with uow:
        dec = uow.files.get_dec_oper_gnl()
        return dec


def get_dec_oper_interc(uow: AbstractUnitOfWork) -> DecOperInterc:
    with uow:
        dec = uow.files.get_dec_oper_interc()
        return dec


def get_avl_turb_max(uow: AbstractUnitOfWork) -> AvlTurbMax:
    with uow:
        avl = uow.files.get_avl_turb_max()
        return avl


def get_dec_fcf_cortes(
    stage: int, uow: AbstractUnitOfWork
) -> Optional[DecFcfCortes]:
    with uow:
        dec = uow.files.get_dec_fcf_cortes(stage)
        return dec
