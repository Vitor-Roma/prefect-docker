import ast
import asyncio
import json
import os
import subprocess
import urllib.request
from pathlib import Path
from prefect.client.orchestration import get_client
from prefect.client.schemas.schedules import construct_schedule


FLOWS_DIRECTORY = Path("/app/app/flows")
WORK_POOL_NAME = "default-pool"
MANAGED_TAG = "auto-managed"

PREFECT_API_URL = os.getenv(
    "PREFECT_API_URL",
    "http://prefect-server:4200/api",
)

FLOW_SCHEDULES = json.loads(
    os.getenv("FLOW_SCHEDULES", "{}")
)


def prefect_request(path, method="GET", data=None):
    url = f"{PREFECT_API_URL}{path}"

    body = None

    if data is not None:
        body = json.dumps(data).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "Content-Type": "application/json",
        },
    )

    with urllib.request.urlopen(request) as response:
        content = response.read()

        if not content:
            return None

        return json.loads(content.decode("utf-8"))


def remove_managed_deployments():
    print("Searching for existing managed deployments...")

    deployments = prefect_request(
        "/deployments/filter",
        method="POST",
        data={},
    )

    for deployment in deployments:
        tags = deployment.get("tags", [])

        if MANAGED_TAG not in tags:
            continue

        deployment_id = deployment["id"]
        deployment_name = deployment["name"]

        print(f"Removing deployment: {deployment_name}")

        prefect_request(
            f"/deployments/{deployment_id}",
            method="DELETE",
        )


def is_flow_decorator(decorator):
    if isinstance(decorator, ast.Name):
        return decorator.id == "flow"

    if isinstance(decorator, ast.Attribute):
        return decorator.attr == "flow"

    if isinstance(decorator, ast.Call):
        return is_flow_decorator(decorator.func)

    return False


def find_flows():
    flows = []

    for file_path in FLOWS_DIRECTORY.rglob("*.py"):
        if file_path.name.startswith("_"):
            continue

        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        for node in tree.body:
            if not isinstance(
                node,
                (ast.FunctionDef, ast.AsyncFunctionDef),
            ):
                continue

            if not any(
                is_flow_decorator(decorator)
                for decorator in node.decorator_list
            ):
                continue

            relative_path = file_path.relative_to("/app")

            flows.append(
                {
                    "name": node.name,
                    "entrypoint": f"{relative_path}:{node.name}",
                }
            )

    return flows

async def create_schedules(flow_name: str, deployment_name: str):
    schedules = FLOW_SCHEDULES.get(flow_name, [])

    if not schedules:
        print(f"No schedules configured for {flow_name}.")
        return

    prefect_flow_name = flow_name.replace("_", "-")

    print(f"Flow name: {prefect_flow_name}")
    print(f"Deployment name: {deployment_name}")
    print(f"Looking for: {prefect_flow_name}/{deployment_name}")

    async with get_client() as client:
        deployment = await client.read_deployment_by_name(
            f"{prefect_flow_name}/{deployment_name}"
        )

        print(f"Deployment found: {deployment.id}")

        for schedule_config in schedules:
            schedule = construct_schedule(
                cron=schedule_config["cron"],
                timezone=schedule_config.get(
                    "timezone",
                    "America/Sao_Paulo",
                ),
            )

            await client.create_deployment_schedules(
                deployment_id=deployment.id,
                schedules=[
                    (schedule, True)
                ],
                parameters=schedule_config.get(
                    "parameters",
                    {},
                ),
            )

            print(
                f"Schedule created: "
                f"{schedule_config['cron']}"
            )

        registered_schedules = (
            await client.read_deployment_schedules(
                deployment.id
            )
        )

        print("Registered schedules:")

        for registered_schedule in registered_schedules:
            print(registered_schedule)

        print("Registered schedules:")

        for registered_schedule in registered_schedules:
            print(registered_schedule)

def deploy_flow(flow):
    deployment_name = flow["name"].replace("_", "-")

    print(f"Deploying: {deployment_name}")

    subprocess.run(
        [
            "prefect",
            "deploy",
            flow["entrypoint"],
            "--name",
            deployment_name,
            "--pool",
            WORK_POOL_NAME,
            "--tag",
            MANAGED_TAG,
        ],
        check=True,
    )

    return deployment_name

def main():
    print("Synchronizing deployments...")

    remove_managed_deployments()

    flows = find_flows()

    print(f"Found {len(flows)} flow(s).")

    for flow in flows:
        deployment_name = deploy_flow(flow)

        asyncio.run(
            create_schedules(
                flow["name"],
                deployment_name,
            )
        )

    print("Deployment synchronization completed.")


if __name__ == "__main__":
    main()