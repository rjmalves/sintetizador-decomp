import os
from pathlib import Path
from types import TracebackType
from typing import Any, Optional, Type


class set_directory:
    """
    Directory changing context manager for helping specific cases
    in HPC script executions.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.origin = Path().absolute()

    def __enter__(self) -> None:
        os.chdir(self.path)

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
        **kwargs: Any,
    ) -> None:
        os.chdir(self.origin)
