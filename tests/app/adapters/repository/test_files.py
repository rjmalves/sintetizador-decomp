from app.adapters.repository.files import factory


import pandas as pd

from tests.conftest import DECK_TEST_DIR


def test_get_dadger(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    dadger = repo.get_dadger()
    assert (
        dadger.te.titulo
        == "PMO - MAIO/24 - JUNHO/24 - REV 0 - FCF COM CVAR - 12 REE - VALOR ESPERADO"
    )


def test_get_dadgnl(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    dadgnl = repo.get_dadgnl()
    assert isinstance(dadgnl.tg(df=True), pd.DataFrame)


def test_get_inviab_unic(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    inviab_unic = repo.get_inviabunic()
    assert isinstance(inviab_unic.inviabilidades_simulacao_final, pd.DataFrame)


def test_get_decomptim(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    decomptim = repo.get_decomptim()
    assert isinstance(decomptim.tempos_etapas, pd.DataFrame)


def test_get_relato(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    relato = repo.get_relato()
    assert isinstance(relato.balanco_energetico, pd.DataFrame)


def test_get_relato2(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    relato = repo.get_relato2()
    assert isinstance(relato.balanco_energetico, pd.DataFrame)


def test_get_relgnl(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    relgnl = repo.get_relgnl()
    assert isinstance(relgnl.relatorio_operacao_termica, pd.DataFrame)


def test_get_hidr(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    hidr = repo.get_hidr()
    assert isinstance(hidr.cadastro, pd.DataFrame)


def test_get_vazoes(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    vaz = repo.get_vazoes()
    assert isinstance(vaz.probabilidades, pd.DataFrame)


def test_get_dec_oper_usih(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_oper_usih()
    assert isinstance(oper.tabela, pd.DataFrame)


def test_get_dec_oper_usit(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_oper_usit()
    assert isinstance(oper.tabela, pd.DataFrame)


def test_get_dec_oper_gnl(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_oper_gnl()
    assert isinstance(oper.tabela, pd.DataFrame)


def test_get_dec_oper_ree(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_oper_ree()
    assert isinstance(oper.tabela, pd.DataFrame)


def test_get_dec_oper_sist(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_oper_sist()
    assert isinstance(oper.tabela, pd.DataFrame)


def test_get_dec_oper_interc(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_oper_interc()
    assert isinstance(oper.tabela, pd.DataFrame)


def test_get_dec_eco_discr(test_settings):
    repo = factory("FS", DECK_TEST_DIR)
    oper = repo.get_dec_eco_discr()
    assert isinstance(oper.tabela, pd.DataFrame)
