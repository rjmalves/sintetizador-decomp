import os
import time
from multiprocessing import Manager
from typing import Any, Tuple

import click

import app.domain.commands as commands
import app.services.handlers as handlers
from app.services.unitofwork import factory
from app.utils.log import Log


def _setup_logging() -> Any:
    """Initialize multiprocessing queue and logging."""
    m = Manager()
    q: Any = m.Queue(-1)
    Log.start_logging_process(q)
    return q


def _log_and_execute(title: str, q: Any, handler_fn: Any, command: Any) -> None:
    """Execute handler with logging setup and teardown."""
    logger = Log.configure_main_logger(q)
    logger.info(f"# {title} #")
    uow = factory("FS", os.curdir, q)
    handler_fn(command, uow)
    logger.info("# Fim da síntese #")
    time.sleep(1.0)
    Log.terminate_logging_process()


@click.group()
def app() -> None:
    """
    Aplicação para realizar a síntese de informações em
    um modelo unificado de dados para o DECOMP.
    """
    pass


@click.command("sistema")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def sistema(variaveis: Tuple[str, ...], formato: str) -> None:
    """Realiza a síntese dos dados do sistema do DECOMP."""
    os.environ["FORMATO_SINTESE"] = formato
    q = _setup_logging()
    _log_and_execute(
        "Realizando síntese do SISTEMA",
        q,
        handlers.synthetize_system,
        commands.SynthetizeSystem(list(variaveis)),
    )


@click.command("execucao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def execucao(variaveis: Tuple[str, ...], formato: str) -> None:
    """Realiza a síntese dos dados da execução do DECOMP."""
    os.environ["FORMATO_SINTESE"] = formato
    q = _setup_logging()
    _log_and_execute(
        "Realizando síntese da EXECUÇÃO",
        q,
        handlers.synthetize_execution,
        commands.SynthetizeExecution(list(variaveis)),
    )


@click.command("cenarios")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def cenarios(variaveis: Tuple[str, ...], formato: str) -> None:
    """Realiza a síntese dos dados de cenários do DECOMP."""
    os.environ["FORMATO_SINTESE"] = formato
    q = _setup_logging()
    _log_and_execute(
        "Realizando síntese dos CENÁRIOS",
        q,
        handlers.synthetize_scenario,
        commands.SynthetizeScenario(list(variaveis)),
    )


@click.command("operacao")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
@click.option(
    "--processadores",
    default=1,
    help="numero de processadores para paralelizar",
)
def operacao(
    variaveis: Tuple[str, ...], formato: str, processadores: int
) -> None:
    """Realiza a síntese dos dados da operação do DECOMP."""
    os.environ["FORMATO_SINTESE"] = formato
    os.environ["PROCESSADORES"] = str(processadores)
    q = _setup_logging()
    _log_and_execute(
        "Realizando síntese da OPERACAO",
        q,
        handlers.synthetize_operation,
        commands.SynthetizeOperation(list(variaveis)),
    )


@click.command("politica")
@click.argument(
    "variaveis",
    nargs=-1,
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
def politica(variaveis: Tuple[str, ...], formato: str) -> None:
    """Realiza a síntese dos dados da política do DECOMP."""
    os.environ["FORMATO_SINTESE"] = formato
    q = _setup_logging()
    _log_and_execute(
        "Realizando síntese da POLITICA",
        q,
        handlers.synthetize_policy,
        commands.SynthetizePolicy(list(variaveis)),
    )


@click.command("limpeza")
def limpeza() -> None:
    """
    Realiza a limpeza dos dados resultantes de uma síntese.
    """
    handlers.clean()


@click.command("completa")
@click.option(
    "--sistema", multiple=True, help="variável do sistema para síntese"
)
@click.option(
    "--execucao", multiple=True, help="variável da execução para síntese"
)
@click.option(
    "--cenarios", multiple=True, help="variável dos cenários para síntese"
)
@click.option(
    "--operacao", multiple=True, help="variável da operação para síntese"
)
@click.option(
    "--politica", multiple=True, help="variável da política para síntese"
)
@click.option(
    "--formato", default="PARQUET", help="formato para escrita da síntese"
)
@click.option(
    "--processadores",
    default=1,
    help="numero de processadores para paralelizar",
)
def completa(
    sistema: Tuple[str, ...],
    execucao: Tuple[str, ...],
    cenarios: Tuple[str, ...],
    operacao: Tuple[str, ...],
    politica: Tuple[str, ...],
    formato: str,
    processadores: int,
) -> None:
    """Realiza a síntese completa do DECOMP."""
    os.environ["FORMATO_SINTESE"] = formato
    os.environ["PROCESSADORES"] = str(processadores)

    m = Manager()
    q: Any = m.Queue(-1)
    Log.start_logging_process(q)

    logger = Log.configure_main_logger(q)
    logger.info("# Realizando síntese COMPLETA #")

    uow = factory("FS", os.curdir, q)
    handlers.synthetize_system(commands.SynthetizeSystem(list(sistema)), uow)
    handlers.synthetize_execution(
        commands.SynthetizeExecution(list(execucao)), uow
    )
    handlers.synthetize_scenario(
        commands.SynthetizeScenario(list(cenarios)), uow
    )
    handlers.synthetize_operation(
        commands.SynthetizeOperation(list(operacao)), uow
    )
    handlers.synthetize_policy(commands.SynthetizePolicy(list(politica)), uow)

    logger.info("# Fim da síntese #")
    time.sleep(1.0)
    Log.terminate_logging_process()


app.add_command(completa)
app.add_command(sistema)
app.add_command(execucao)
app.add_command(cenarios)
app.add_command(operacao)
app.add_command(politica)
app.add_command(limpeza)
