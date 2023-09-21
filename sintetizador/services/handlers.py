import pathlib
import shutil
from sintetizador.model.settings import Settings
import sintetizador.domain.commands as commands
from sintetizador.services.unitofwork import AbstractUnitOfWork
from sintetizador.services.synthesis.system import SystemSynthetizer
from sintetizador.services.synthesis.execution import ExecutionSynthetizer
from sintetizador.services.synthesis.scenarios import ScenarioSynthetizer
from sintetizador.services.synthesis.operation import OperationSynthetizer


def synthetize_system(
    command: commands.SynthetizeSystem, uow: AbstractUnitOfWork
):
    synthetizer = SystemSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def synthetize_execution(
    command: commands.SynthetizeExecution, uow: AbstractUnitOfWork
):
    synthetizer = ExecutionSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def synthetize_scenario(
    command: commands.SynthetizeScenario, uow: AbstractUnitOfWork
):
    synthetizer = ScenarioSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
):
    synthetizer = OperationSynthetizer()
    synthetizer.synthetize(command.variables, uow)


def clean():
    path = pathlib.Path(Settings().basedir).joinpath(Settings().synthesis_dir)
    shutil.rmtree(path)
