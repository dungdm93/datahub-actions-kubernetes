import logging
from typing import Generator, Optional

from aiohttp import ClientResponse
from kubernetes_asyncio import watch
from kubernetes_asyncio.client.api import CoreV1Api
from kubernetes_asyncio.client.exceptions import ApiException
from kubernetes_asyncio.client.models import V1Pod, V1Status

logger = logging.getLogger(__name__)


class PodPhase:
    PENDING = "Pending"
    RUNNING = "Running"
    FAILED = "Failed"
    SUCCEEDED = "Succeeded"
    UNKNOWN = "Unknown"


class EventType:
    ERROR = "ERROR"
    ADDED = "ADDED"
    MODIFIED = "MODIFIED"
    DELETED = "DELETED"


def get_pod_state(pod: V1Pod) -> Optional[str]:
    """Return Pod phase"""
    if not pod.status:
        return None
    return pod.status.phase  # type: ignore


async def stream_pod_event(core_api: CoreV1Api, pod: V1Pod) -> Generator[V1Pod, None, None]:
    while True:
        try:
            async with watch.Watch().stream(
                core_api.list_namespaced_pod,
                namespace=pod.metadata.namespace,
                field_selector=f"metadata.name={pod.metadata.name}",
                # timeout_seconds=10 * 60,
            ) as stream:
                async for event in stream:
                    if event["type"] == EventType.ERROR:
                        logging.error("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                        status: V1Status = event["object"]
                        raise ApiException(status=status.code, reason=f"{status.reason}: {status.message}")
                    elif event["type"] == EventType.DELETED:
                        logging.warning("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                        logging.warning("pod=%s has gone", pod.metadata.name)
                        return

                    # event["type"] = "ADDED" | "MODIFIED"
                    logging.debug("Event: type=%s, raw_object=%s", event["type"], event["raw_object"])
                    yield event["object"]
        except ApiException as e:
            if e.status != 410:
                raise
            logging.exception(e)
            # https://kubernetes.io/docs/reference/using-api/api-concepts/#the-resourceversion-parameter
            logging.warning("Kubernetes resourceVersion is too old, let's retry w/ most recent event")


async def read_pod_logs(core_api: CoreV1Api, pod: V1Pod,
                        tail_lines: int = None,
                        timestamps: bool = None,
                        since_seconds: int = None) -> Generator[str, None, None]:
    """Reads log from the POD"""
    additional_kwargs = {}
    if since_seconds:
        additional_kwargs["since_seconds"] = since_seconds
    if timestamps:
        additional_kwargs["timestamps"] = timestamps
    if tail_lines:
        additional_kwargs["tail_lines"] = tail_lines

    async with (await core_api.read_namespaced_pod_log(  # noqa
        name=pod.metadata.name,
        namespace=pod.metadata.namespace,
        follow=True,
        _preload_content=False,
        **additional_kwargs
    )) as resp:  # type:ClientResponse
        if not resp.ok:
            return
        while True:
            line = await resp.content.readline()
            if not line:
                break
            yield line.decode("utf-8").strip("\r\n")


async def monitor_pod(core_api: CoreV1Api, pod: V1Pod) -> None:
    async for p in stream_pod_event(core_api, pod):
        state = get_pod_state(p)
        logging.info("pod=%s state=%s", p.metadata.name, state)

        if state in (PodPhase.SUCCEEDED, PodPhase.FAILED):
            break
        if state != PodPhase.RUNNING:
            continue

        # Pod is running
        async for line in read_pod_logs(core_api, p, tail_lines=10):
            logging.info("pod | %s", line)
