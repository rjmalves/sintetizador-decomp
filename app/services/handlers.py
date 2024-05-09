import pathlib
import shutil
from app.model.settings import Settings
import sintetizador.domain.commands as commands
from app.services.unitofwork import AbstractUnitOfWork
from app.services.synthesis.system import SystemSynthetizer
from app.services.synthesis.execution import ExecutionSynthetizer
from app.services.synthesis.scenarios import ScenarioSynthetizer
from app.services.synthesis.operation import OperationSynthetizer


def synthetize_system(
    command: commands.SynthetizeSystem, uow: AbstractUnitOfWork
):
    SystemSynthetizer.synthetize(command.variables, uow)


def synthetize_execution(
    command: commands.SynthetizeExecution, uow: AbstractUnitOfWork
):
    ExecutionSynthetizer.synthetize(command.variables, uow)


def synthetize_scenario(
    command: commands.SynthetizeScenario, uow: AbstractUnitOfWork
):
    ScenarioSynthetizer.synthetize(command.variables, uow)


def synthetize_operation(
    command: commands.SynthetizeOperation, uow: AbstractUnitOfWork
):
    OperationSynthetizer.synthetize(command.variables, uow)


def clean():
    path = pathlib.Path(Settings().basedir).joinpath(Settings().synthesis_dir)
    shutil.rmtree(path)
