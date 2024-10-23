from abc import ABCMeta, abstractmethod
from typing import Any, Optional, Union


class BaseClient(metaclass=ABCMeta):
    @abstractmethod
    def run_remote_script(
        self,
        script_file: str,
        script_dir: str,
        args: str,
        storage_account: str,
        subscription: str,
        wait_for_completion: bool = True,
    ):
        pass

    @abstractmethod
    def run_pipeline(
        self, pipeline_name: str, pipeline_parameters: Optional[dict] = None
    ) -> Union[str, int]:
        pass

    @abstractmethod
    def wait_for_pipeline(self, result: str, running_pipeline: Any):
        pass
