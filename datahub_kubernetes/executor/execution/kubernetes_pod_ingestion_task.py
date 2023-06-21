import logging
from collections import deque
from os import getenv
from typing import Tuple, List

import yaml
from acryl.executor.common.config import ConfigModel, PermissiveConfigModel
from acryl.executor.context.execution_context import ExecutionContext
from acryl.executor.context.executor_context import ExecutorContext
from acryl.executor.execution.sub_process_task_common import SubProcessTaskUtil
from acryl.executor.execution.task import Task
from datahub.cli.cli_utils import (
    ENV_SKIP_CONFIG,
    ENV_METADATA_HOST_URL,
    ENV_METADATA_HOST,
    ENV_METADATA_PORT,
    ENV_METADATA_PROTOCOL,
    ENV_METADATA_TOKEN,
    ENV_DATAHUB_SYSTEM_CLIENT_ID,
    ENV_DATAHUB_SYSTEM_CLIENT_SECRET,
)
from kubernetes_asyncio import config as kube
from kubernetes_asyncio.client import V1EnvVar
from kubernetes_asyncio.client.api import CoreV1Api
from kubernetes_asyncio.client.api_client import ApiClient as K8sApiClient
from kubernetes_asyncio.client.models import (
    V1Pod, V1ObjectMeta, V1PodSpec, V1Container, V1Secret,
    V1VolumeMount, V1Volume, V1SecretVolumeSource
)

from .utils import (
    PodPhase, get_pod_state, stream_pod_event, read_pod_logs,
)

logger = logging.getLogger(__name__)


class KubernetesConfig(ConfigModel):
    in_cluster: bool = False
    config_file: str = None
    config_dict: dict = None
    context: str = None
    namespace: str = "default"


class KubernetesPodIngestionTaskConfig(ConfigModel):
    k8s_config: KubernetesConfig = KubernetesConfig()
    image_template: str
    max_log_lines: int = SubProcessTaskUtil.MAX_LOG_LINES


class KubernetesPodIngestionTaskArgs(PermissiveConfigModel):
    recipe: str
    version: str = "latest"
    debug_mode: str = "false"  # Expected values are "true" or "false".


class KubernetesPodIngestionTask(Task):
    config: KubernetesPodIngestionTaskConfig

    @classmethod
    def create(cls, config: dict, ctx: ExecutorContext) -> "Task":
        config = KubernetesPodIngestionTaskConfig.parse_obj(config)
        return cls(config, ctx)

    def __init__(self, config: KubernetesPodIngestionTaskConfig, ctx: ExecutorContext):
        self.config = config
        self.ctx = ctx

    async def k8s_client(self) -> K8sApiClient:
        k8s_config = self.config.k8s_config
        if k8s_config.in_cluster:
            kube.load_incluster_config()
        elif k8s_config.config_dict:
            await kube.load_kube_config_from_dict(
                config_dict=k8s_config.config_dict,
                context=k8s_config.context
            )
        else:
            await kube.load_kube_config(
                config_file=k8s_config.config_file,
                context=k8s_config.context
            )
        return K8sApiClient()

    @staticmethod
    def update_ingest_container_env(container: V1Container, env_name: str):
        env_value = getenv(env_name)
        if env_value is None:
            return

        if not container.env:
            container.env = list()
        env: List[V1EnvVar] = container.env

        for e in env:
            if e.name == env_name:
                e.value = env_value
                return

        env.append(V1EnvVar(
            name=env_name,
            value=env_value
        ))

    def build_k8s_objects(self,
                          args: KubernetesPodIngestionTaskArgs,
                          ctx: ExecutionContext,
                          ) -> Tuple[str, V1Pod, V1Secret]:
        recipe: dict = SubProcessTaskUtil._resolve_recipe(args.recipe, ctx, self.ctx)  # noqa
        pod = V1Pod()
        name = f"datahub-ingest-{ctx.exec_id}"

        if not pod.metadata:
            pod.metadata = V1ObjectMeta()
        metadata: V1ObjectMeta = pod.metadata
        metadata.name = name
        metadata.namespace = self.config.k8s_config.namespace
        if not metadata.labels:
            metadata.labels = {}
        metadata.labels.update({
            "datahubproject.io/source_type": recipe["source"]["type"],
            "datahubproject.io/exec_id": ctx.exec_id,
            "datahubproject.io/version": args.version
        })

        if not pod.spec:
            pod.spec = V1PodSpec(containers=[])
        spec: V1PodSpec = pod.spec
        spec.restart_policy = "Never"

        if not spec.containers:
            spec.containers = []
        for c in spec.containers:  # type:V1Container
            if c.name == "datahub-ingest":
                ingest_c = c
                break
        else:
            ingest_c = V1Container(name="datahub-ingest")
            spec.containers.append(ingest_c)
        ingest_c.image = self.config.image_template.format(version=args.version, type=recipe["source"]["type"])
        ingest_c.args = ["datahub", "ingest", "run", "--no-default-report", "-c", "/etc/datahub/recipe.yaml"]

        # See: datahub.ingestion.run.pipeline_config.PipelineConfig.default_sink_is_datahub_rest
        #       -> get_url_and_token
        #       -> datahub.cli.cli_utils.get_details_from_env
        self.update_ingest_container_env(ingest_c, ENV_SKIP_CONFIG)
        self.update_ingest_container_env(ingest_c, ENV_METADATA_HOST_URL)
        self.update_ingest_container_env(ingest_c, ENV_METADATA_HOST)
        self.update_ingest_container_env(ingest_c, ENV_METADATA_PORT)
        self.update_ingest_container_env(ingest_c, ENV_METADATA_PROTOCOL)
        self.update_ingest_container_env(ingest_c, ENV_METADATA_TOKEN)
        self.update_ingest_container_env(ingest_c, ENV_DATAHUB_SYSTEM_CLIENT_ID)
        self.update_ingest_container_env(ingest_c, ENV_DATAHUB_SYSTEM_CLIENT_SECRET)

        secret = V1Secret(
            api_version="v1",
            kind="Secret",
            metadata=V1ObjectMeta(
                name=name,
                namespace=self.config.k8s_config.namespace,
                labels={
                    "datahubproject.io/source_type": recipe["source"]["type"],
                    "datahubproject.io/exec_id": ctx.exec_id,
                    "datahubproject.io/version": args.version
                },
            ),
            string_data={
                "recipe.yaml": yaml.dump(recipe)
            }
        )

        if not ingest_c.volume_mounts:
            ingest_c.volume_mounts = []
        for vm in ingest_c.volume_mounts:  # type:V1VolumeMount
            if vm.name == "recipe":
                recipe_vm = vm
                break
        else:
            recipe_vm = V1VolumeMount(name="recipe", mount_path="/etc/datahub/recipe.yaml")
            ingest_c.volume_mounts.append(recipe_vm)
        recipe_vm.mount_path = "/etc/datahub/recipe.yaml"
        recipe_vm.sub_path = "recipe.yaml"

        if not spec.volumes:
            spec.volumes = []
        for v in spec.volumes:  # type:V1Volume
            if v.name == "recipe":
                spec.volumes.remove(v)
                break
        recipe_v = V1Volume(
            name="recipe",
            secret=V1SecretVolumeSource(secret_name=f"datahub-ingest-{ctx.exec_id}")
        )
        spec.volumes.append(recipe_v)

        return name, pod, secret

    @staticmethod
    async def cleanup_k8s_objects(ctx: ExecutionContext, core_api: CoreV1Api, name: str, namespace: str):
        try:
            await core_api.delete_namespaced_secret(name, namespace)
            await core_api.delete_namespaced_pod(name, namespace)
        except Exception as e:
            logger.error("[exec_id=%s] failed to cleanup k8s resources", ctx.exec_id)
            logger.exception(e)

    async def execute(self, args: dict, ctx: ExecutionContext) -> None:
        validated_args = KubernetesPodIngestionTaskArgs.parse_obj(args)
        name, pod, secret = self.build_k8s_objects(validated_args, ctx)
        namespace = self.config.k8s_config.namespace

        k8s_client = await self.k8s_client()
        core_api = CoreV1Api(k8s_client)
        logs_deque: deque[str] = deque(maxlen=self.config.max_log_lines)
        logs_truncated = False

        try:
            await core_api.create_namespaced_secret(namespace, secret)
            await core_api.create_namespaced_pod(namespace, pod)

            async for p in stream_pod_event(core_api, pod):
                state = get_pod_state(p)
                logger.info("[exec_id=%s] pod=%s state=%s", ctx.exec_id, p.metadata.name, state)

                if state == PodPhase.SUCCEEDED:
                    break
                if state == PodPhase.FAILED:
                    raise RuntimeError(f"Something went wrong in the datahub ingestion pod \"{name}\"")
                if state != PodPhase.RUNNING:
                    continue

                # Pod is running
                async for line in read_pod_logs(core_api, p, tail_lines=100):
                    logs_deque.append(line)
                    while len(logs_deque) > SubProcessTaskUtil.MAX_LOG_LINES:
                        logs_deque.popleft()
                        logs_truncated = True
                    logs = "\n".join(logs_deque)
                    if logs_truncated:
                        logs = "[earlier logs truncated...]\n" + logs
                    ctx.request.progress_callback(logs)

            ctx.get_report().report_info("Successfully executed 'datahub ingest'")
        except Exception as e:  # noqa
            ctx.get_report().report_info("Failed to execute 'datahub ingest'")
            raise
        finally:
            logs = "\n".join(logs_deque)
            if logs_truncated:
                logs = "[earlier logs truncated...]\n" + logs
            ctx.get_report().set_logs(logs)
            if validated_args.debug_mode.lower() == "false":  # not in DEBUG mode
                await self.cleanup_k8s_objects(ctx, core_api, name, namespace)
            await k8s_client.close()

    def close(self) -> None:
        pass
