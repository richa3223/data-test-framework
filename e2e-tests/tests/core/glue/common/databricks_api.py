from base64 import b64encode
import logging
from enum import Enum
from pathlib import Path
import time
from typing import Any, Dict, List, Optional, Set
from uuid import uuid1
from filelock import FileLock

import requests
from azure.identity import DefaultAzureCredential

from core.glue.common.azure import is_running_as_sp, timed_cache
from core.glue.common.base_api import BaseClient
from core.glue.common.utils import TerminalColours

import json


class RequestTypes(Enum):
    GET = "GET"
    POST = "POST"
    PATCH = "PATCH"
    PUT = "PUT"


class DatabricksClient(BaseClient):
    def __init__(
        self,
        databricks_url: str,
        token_lock_file: Path,
        credential: DefaultAzureCredential = None,
    ) -> None:
        self.endpoint = databricks_url
        self._credential = credential or DefaultAzureCredential()
        self._token_lock_file = token_lock_file

    @timed_cache(minutes=5)
    def get_databricks_token(self):
        logging.warning("Refreshed databricks token")
        with FileLock(str(self._token_lock_file)):
            return self._credential.get_token(
                "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
            ).token

    def _query(
        self, path: str, type: str = RequestTypes.GET, json: Optional[dict] = None
    ) -> requests.Response:
        logging.debug(f"Calling {self.endpoint}{path}")
        funcs = {
            RequestTypes.GET: requests.get,
            RequestTypes.POST: requests.post,
            RequestTypes.PATCH: requests.patch,
            RequestTypes.PUT: requests.put,
        }
        response = funcs[type](
            f"{self.endpoint}{path}",
            headers={"Authorization": f"Bearer {self.get_databricks_token()}"},
            json=json,
        )
        logging.debug(f"Response from {self.endpoint}{path}: {response}")
        if response.status_code != 200:
            print(response.text)
            raise Exception(f"invalid response {response.text}")
        return response

    def get_cluster_id_from_name(self, cluster_name: str) -> str:
        logging.info(f"Finding cluster_id for name {cluster_name}")
        response = self._query("/api/2.0/clusters/list")
        clusters = response.json()
        for cluster in clusters.get("clusters", []):
            if cluster.get("cluster_name") == cluster_name:
                cluster_id = cluster.get("cluster_id")
                logging.info(f"Found cluster_id for name {cluster_name}: {cluster_id}")
                return cluster_id
        raise ValueError(f"No cluster found with name {cluster_name}")

    def get_all_clusters(self) -> dict:
        logging.info("Requesting info for all clusters")
        response = self._query("/api/2.0/clusters/list")
        return response.json()

    def get_cluster(self, cluster_id: str) -> dict:
        logging.info(f"Requesting info for cluster {cluster_id}")
        response = self._query(f"/api/2.0/clusters/get?cluster_id={cluster_id}")
        return response.json()

    def start_cluster(self, cluster_id: str) -> dict:
        logging.info(f"Starting cluster {cluster_id}")
        self._query(
            "/api/2.0/clusters/start",
            json={"cluster_id": cluster_id},
            type=RequestTypes.POST,
        )

    def restart_cluster(self, cluster_id: str) -> dict:
        logging.info(f"Restarting cluster {cluster_id}")
        self._query(
            "/api/2.0/clusters/restart",
            json={"cluster_id": cluster_id},
            type=RequestTypes.POST,
        )

    def stop_cluster(self, cluster_id: str) -> dict:
        logging.info(f"Stopping cluster {cluster_id}")
        self._query(
            "/api/2.0/clusters/delete",
            json={"cluster_id": cluster_id},
            type=RequestTypes.POST,
        )

    def pin_cluster(self, cluster_id: str) -> dict:
        logging.info(f"Pinning cluster {cluster_id}")
        self._query(
            "/api/2.0/clusters/pin",
            json={"cluster_id": cluster_id},
            type=RequestTypes.POST,
        )

    def create_cluster(self, cluster_config: dict) -> dict:
        logging.info(f"Creating new cluster {cluster_config.get('cluster_name')}")
        logging.debug(f"Creating new cluster with config {cluster_config}")
        return self._query(
            "/api/2.0/clusters/create",
            json=cluster_config,
            type=RequestTypes.POST,
        ).json()

    def edit_cluster(self, cluster_config: dict) -> dict:
        logging.info(
            f"Editing cluster {cluster_config.get('cluster_name')}"
            f" {cluster_config.get('cluster_id')}"
        )
        logging.debug(f"Editing cluster with config {cluster_config}")
        self._query(
            "/api/2.0/clusters/edit",
            json=cluster_config,
            type=RequestTypes.POST,
        )

    def get_cluster_library_status(self, cluster_id: str) -> dict:
        logging.info(f"Getting libraries for cluster {cluster_id}")
        response = self._query(
            f"/api/2.0/libraries/cluster-status?cluster_id={cluster_id}"
        )
        return response.json()

    def install_libraries_on_cluster(
        self, cluster_id: str, libraries: List[Dict[str, str]]
    ):
        logging.info(f"Installing libraries on cluster {cluster_id}: {libraries}")
        self._query(
            "/api/2.0/libraries/install",
            json={"cluster_id": cluster_id, "libraries": libraries},
            type=RequestTypes.POST,
        )

    def uninstall_libraries_from_cluster(
        self, cluster_id: str, libraries: List[Dict[str, str]]
    ):
        logging.info(f"Uninstalling libraries from cluster {cluster_id}: {libraries}")
        self._query(
            "/api/2.0/libraries/uninstall",
            json={"cluster_id": cluster_id, "libraries": libraries},
            type=RequestTypes.POST,
        )

    def get_all_existing_jobs(self):
        def get_jobs(offset):
            return self._query(f"/api/2.1/jobs/list?limit=25&offset={offset}").json()

        logging.info("Getting existing jobs")
        offset = 0
        jobs = []
        while True:
            response = get_jobs(offset)
            jobs.extend(response.get("jobs", []))
            if response.get("has_more") is False:
                break
            offset += 25
        logging.info(f"Found {len(jobs)} existing jobs")
        return jobs

    def get_job_id_from_job_name(self, name: str) -> int:
        all_jobs = self.get_all_existing_jobs()
        for job in all_jobs:
            if job.get("settings", {}).get("name") == name:
                return job["job_id"]
        raise ValueError(f"No Pipeline found with name '{name}'")

    def get_job(self, job_id: int) -> dict:
        logging.info(f"Getting information about job {job_id}")
        response = self._query(f"/api/2.1/jobs/get?job_id={job_id}")
        return response.json()

    def create_job(self, job_config: dict) -> str:
        logging.info(f"Creating new job {job_config['settings']['name']}")
        return (
            self._query(
                "/api/2.1/jobs/create",
                json=job_config["settings"],
                type=RequestTypes.POST,
            )
            .json()
            .get("job_id")
        )

    def edit_job(self, existing_job_id: int, job_config: dict):
        logging.info(f"Editing existing job {job_config['settings']['name']}")
        self._query(
            "/api/2.1/jobs/reset",
            json={"job_id": existing_job_id, "new_settings": job_config["settings"]},
            type=RequestTypes.POST,
        )

    def get_job_acls(self, job_id: str):
        logging.info(f"Requesting job ACLs for job {job_id}")
        return (
            self._query(f"/api/2.0/permissions/jobs/{job_id}")
            .json()
            .get("access_control_list")
        )

    def patch_job_acls(self, job_id: str, acls: List[Dict[str, str]]):
        logging.info(f"Patching permissions for job {job_id}")
        logging.debug(f"Applying acls {acls}")
        self._query(
            f"/api/2.0/permissions/jobs/{job_id}",
            json={"access_control_list": acls},
            type=RequestTypes.PATCH,
        )

    def overwrite_directory_acls(
        self, object_id: int, access_control_list: List[Dict[str, str]]
    ):
        logging.info(f"Overwriting acls for directory {object_id}")
        logging.debug(f"Applying acls: {access_control_list}")
        self._query(
            f"/api/2.0/permissions/directories/{object_id}",
            json={"access_control_list": access_control_list},
            type=RequestTypes.PUT,
        )

    def patch_directory_acls(
        self, object_id: int, access_control_list: List[Dict[str, str]]
    ):
        logging.info(f"Patching acls for directory {object_id}")
        logging.debug(f"Applying acls: {access_control_list}")
        self._query(
            f"/api/2.0/permissions/directories/{object_id}",
            json={"access_control_list": access_control_list},
            type=RequestTypes.PATCH,
        )

    def get_sp_id_from_name(self, sp_name: str) -> str:
        logging.info(f"Getting SP ID for SP: {sp_name}")
        result = self._query("/api/2.0/preview/scim/v2/ServicePrincipals")
        sp_names_by_id = {
            resource.get("displayName", resource["applicationId"]): resource[
                "applicationId"
            ]
            for resource in result.json().get("Resources", [])
        }
        if sp_name not in sp_names_by_id:
            raise ValueError(
                f"No SP found with name {sp_name}, available sps:\n{list(sp_names_by_id.keys())}"
            )
        return sp_names_by_id[sp_name]

    def get_me(self):
        logging.info("Requesting info on current user")
        return self._query("/api/2.0/preview/scim/v2/Me").json()

    def get_object_id_from_folder_path(self, folder_path: str) -> int:
        logging.info(f"Requesting object_id for directory {folder_path}")
        response = self._query(
            "/api/2.0/workspace/get-status",
            json={"path": folder_path},
        )
        return response.json()["object_id"]

    def get_node_types(self) -> List[dict]:
        logging.info("Requesting node types")
        response = self._query(
            "/api/2.0/clusters/list-node-types",
        )
        return response.json()["node_types"]

    def get_photon_compatible_node_types(self) -> Set[str]:
        return {
            node["node_type_id"]
            for node in self.get_node_types()
            if node.get("photon_driver_capable")
        }

    def make_notebook_dir(self, remote_path: str):
        logging.info(f"Creating databricks directory {remote_path} if not exists")
        self._query(
            "/api/2.0/workspace/mkdirs",
            type=RequestTypes.POST,
            json={"path": remote_path},
        )

    def delete_notebook_directory(self, remote_path: str, recursive: bool = False):
        logging.info(f"Deleting databricks directory {remote_path}")
        self._query(
            "/api/2.0/workspace/delete",
            type=RequestTypes.POST,
            json={"path": remote_path, "recursive": recursive},
        )

    def upload_python_notebook(
        self, local_path: Path, remote_path: str, overwrite: bool = True
    ):
        logging.info(f"Uploading Notebook {local_path} to {remote_path}")
        with open(local_path, "rb") as f:
            content = b64encode(f.read()).decode()
        self._query(
            "/api/2.0/workspace/import",
            type=RequestTypes.POST,
            json={
                "path": remote_path,
                "content": content,
                "language": "PYTHON",
                "format": "SOURCE",
                "overwrite": overwrite,
            },
        )

    def upload_notebook_folder(
        self, local_path: Path, remote_path: str, overwrite: bool = True
    ):
        local_path = local_path.absolute().resolve()
        logging.info(
            f"Syncing local folder {local_path} to databricks at {remote_path}"
        )
        self.make_notebook_dir(f"/{remote_path}")
        for path in local_path.glob("*"):
            if path.name.startswith("."):
                logging.info(f"Skipping hidden file {path}")
                continue
            if path.is_dir():
                self.upload_notebook_folder(
                    path, f"/{remote_path}/{path.relative_to(local_path)}", overwrite
                )
            elif path.is_file():
                if path.suffix == ".py":
                    self.upload_python_notebook(
                        path,
                        f"/{remote_path}/{path.relative_to(local_path)}",
                        overwrite,
                    )
                else:
                    logging.warning(
                        f"No upload method for file extension {path.suffix}, skipping upload"
                    )

    def run_one_time_job(self, job_config: dict) -> dict:
        logging.info("Starting one time job")
        response = self._query(
            "/api/2.1/jobs/runs/submit", type=RequestTypes.POST, json=job_config
        )
        return response.json()

    def get_job_run(self, run_id) -> dict:
        logging.debug(f"Getting info for job {run_id}")
        response = self._query(
            f"/api/2.1/jobs/runs/get?run_id={run_id}&include_history=false"
        )
        return response.json()

    def cancel_job_run(self, run_id) -> dict:
        logging.warning(f"Cancelling job {run_id}")
        self._query(
            f"/api/2.1/jobs/runs/cancel",
            type=RequestTypes.POST,
            json={"run_id": run_id},
        )

    def get_job_output(self, run_id) -> dict:
        logging.debug(f"Getting output for job {run_id}")
        response = self._query(f"/api/2.1/jobs/runs/export?run_id={run_id}")
        return response.json()

    def trigger_existing_job(
        self, job_id: int, parameters: Optional[Dict[str, str]] = None
    ) -> dict:
        logging.info(f"Triggering job {job_id}")
        params = {"job_id": job_id}
        if parameters:
            params["notebook_params"] = parameters
        response = self._query(
            f"/api/2.1/jobs/run-now", type=RequestTypes.POST, json=params
        )
        return response.json()

    def wait_for_libraries_to_install(self, cluster_id: str):
        logging.info("Waiting for libraries to finish installing")
        for i in range(max_tries := 120):
            if status := self.get_cluster(cluster_id)["state"] in (
                "TERMINATED",
                "RUNNING",
            ):
                if status == "TERMINATED":
                    raise Exception("Cluster failed to start")
                break
            logging.info(
                f"Waiting for cluster {cluster_id} to finish starting. Try {i+1}/{max_tries}"
            )
            time.sleep(10)
        else:
            raise TimeoutError("Cluster spent too much time starting")
        for i in range(max_tries := 30):

            cluster_library_status = self.get_cluster_library_status(cluster_id)
            if all(
                status["status"]
                in ("FAILED", "SKIPPED", "INSTALLED", "UNINSTALL_ON_RESTART")
                for status in cluster_library_status["library_statuses"]
            ):
                if any(
                    status["status"] != "INSTALLED"
                    for status in cluster_library_status["library_statuses"]
                ):
                    raise Exception(
                        "Some libraries failed to install:"
                        f"\n{json.dumps(cluster_library_status, indent=True)}"
                    )
                break
            if i % 5 == 0:
                logging.info(
                    "Waiting for libraries to install:"
                    f"\n{json.dumps(cluster_library_status, indent=True)}\nTry {i+1}/{max_tries}"
                )
            time.sleep(10)
        else:
            raise TimeoutError("Libraries spent too much time installing on cluster")

    def run_remote_script(
        self,
        script_file: str,
        script_dir: str,
        args: str,
        storage_account: str,
        subscription: str,
        wait_for_completion: bool = True,
    ):
        file = f"{script_dir}/{script_file}"

        logging.info(f"Running synapse_script {script_file} remotely")
        existing_cluster_id = (
            self.get_cluster_id_from_name("QA_Common_1")
            if not is_running_as_sp(subscription)
            else self.get_cluster_id_from_name("QA_CI_CD")
        )
        state = self.get_cluster(existing_cluster_id)["state"]
        if state not in ("RESTARTING", "RUNNING", "PENDING"):
            logging.warning("Shared cluster not yet running, waiting for it to start")
            self.start_cluster(existing_cluster_id)
        logging.info("Check libraries are all installed on shared cluster")
        self.wait_for_libraries_to_install(existing_cluster_id)
        name = script_file.rsplit("/", 1)[-1].split(".", 1)[0] + f"_{uuid1().hex}"
        task_config = {
            "task_key": name,
            "notebook_task": {
                "notebook_path": f"/_synergy_testing/{file}",
                "base_parameters": {"encoded_args": args},
            },
            "existing_cluster_id": existing_cluster_id,
            "timeout_seconds": 0,
            "email_notifications": {},
        }

        job_params = {
            "run_name": name,
            "timeout_seconds": 0,
            "tasks": [task_config],
            "format": "MULTI_TASK",
        }
        if is_running_as_sp(subscription):
            me = self.get_me()
            job_params["access_control_list"] = [
                {"group_name": "developers", "permission_level": "CAN_VIEW"},
                {
                    "service_principal_name": self.get_sp_id_from_name(
                        "SynergyDatabricksSPN"
                    ),
                    "permission_level": "IS_OWNER",
                },
                {
                    "service_principal_name": me["userName"],
                    "permission_level": "CAN_MANAGE_RUN",
                },
            ]
        response = self.run_one_time_job(job_params)
        run_id = response.get("run_id")
        logging.info(f"Batch job running as run_id: {run_id}")
        if not wait_for_completion:
            logging.info(
                f"Batch job {run_id} running asynchronously. Not waiting for completion"
            )
            return
        completed = False
        try:
            start_time = time.time()
            for attempt in range(180):
                response = self.get_job_run(run_id)
                if attempt == 0:
                    logging.info(f"Batch Job Running at {response.get('run_page_url')}")
                if attempt % 3 == 0:
                    logging.info(
                        f"Waiting for batch job {name} to finish running."
                        f" Attempt {attempt+1}/{180} ({time.time() - start_time:0.2f}s elapsed)"
                    )
                    logging.info(
                        response.get("tasks", [{}])[0]
                        .get("state", {})
                        .get("life_cycle_state")
                    )
                if response.get("state").get("life_cycle_state") not in (
                    "RUNNING",
                    "PENDING",
                    "TERMINATING",
                ):
                    completed = True
                    if response.get("state").get("result_state") != "SUCCESS":
                        raise Exception(
                            "Batch job didn't complete, check web UI "
                            + response.get("run_page_url")
                        )
                    break

                time.sleep(5)
            else:
                raise Exception(
                    f"TIMEOUT, batch job {run_id} did not complete after 30 minutes"
                )
        finally:
            if not completed:
                self.cancel_job_run(run_id)

    def run_pipeline(
        self, pipeline_name: str, pipeline_parameters: Optional[dict] = None
    ) -> int:
        job_id = self.get_job_id_from_job_name(pipeline_name)
        job_spec = self.get_job(job_id)
        response = self.trigger_existing_job(job_id, pipeline_parameters)
        return response["run_id"]

    def wait_for_pipeline(self, result: str, running_pipeline: Any):
        completed = False
        try:
            start_time = time.time()
            for attempt in range(1800):
                response = self.get_job_run(running_pipeline)

                if attempt == 0:
                    logging.info(f"Batch Job Running at {response.get('run_page_url')}")
                if attempt % 6 == 0:
                    logging.info(
                        f"Waiting for job {running_pipeline} to finish running."
                        f" Attempt {attempt+1}/{180} ({time.time() - start_time:0.2f}s elapsed)"
                    )
                if response.get("state").get("life_cycle_state") not in (
                    "RUNNING",
                    "PENDING",
                    "TERMINATING",
                ):
                    completed = True
                    result_state = response.get("state").get("result_state")
                    if not result_state == result:
                        raise AssertionError(
                            f"Pipeline final state did not match {result}, got {result_state}"
                        )
                    return

                time.sleep(10)
            else:
                raise Exception(
                    f"TIMEOUT, batch job {running_pipeline} did not complete after 30 minutes"
                )
        finally:
            if not completed:
                self.cancel_job_run(running_pipeline)
