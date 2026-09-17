#!/usr/bin/env python3
"""ShardStore CLI - Python client for the distributed object storage."""

import typer
import httpx
from rich.console import Console
from rich.table import Table
from pathlib import Path

app = typer.Typer(name="shard-store", help="Distributed object storage CLI")
bucket_app = typer.Typer(help="Bucket operations")
object_app = typer.Typer(help="Object operations")
cluster_app = typer.Typer(help="Cluster operations")

app.add_typer(bucket_app, name="bucket")
app.add_typer(object_app, name="object")
app.add_typer(cluster_app, name="cluster")

console = Console()

COORDINATOR_URL = "http://localhost:8080"


def get_base_url() -> str:
    import os
    return os.environ.get("SHARD_STORE_URL", COORDINATOR_URL)


@bucket_app.command("create")
def bucket_create(name: str):
    """Create a new bucket."""
    r = httpx.put(f"{get_base_url()}/buckets/{name}")
    if r.status_code == 201:
        console.print(f"[green]Bucket '{name}' created.[/green]")
    else:
        console.print(f"[red]Error: {r.text}[/red]")
        raise typer.Exit(1)


@bucket_app.command("delete")
def bucket_delete(name: str):
    """Delete a bucket."""
    r = httpx.delete(f"{get_base_url()}/buckets/{name}")
    if r.status_code == 200:
        console.print(f"[green]Bucket '{name}' deleted.[/green]")
    else:
        console.print(f"[red]Error: {r.text}[/red]")
        raise typer.Exit(1)


@bucket_app.command("list")
def bucket_list():
    """List all buckets."""
    r = httpx.get(f"{get_base_url()}/buckets")
    data = r.json()
    table = Table(title="Buckets")
    table.add_column("Name")
    for b in data.get("buckets", []):
        table.add_row(b)
    console.print(table)


@object_app.command("put")
def object_put(bucket: str, key: str, file: Path):
    """Upload an object to a bucket."""
    if not file.exists():
        console.print(f"[red]File not found: {file}[/red]")
        raise typer.Exit(1)

    data = file.read_bytes()
    r = httpx.put(f"{get_base_url()}/objects/{bucket}/{key}", content=data, timeout=30.0)
    if r.status_code == 201:
        info = r.json()
        console.print(f"[green]Uploaded {key} to {bucket}[/green]")
        console.print(f"  Size: {info.get('size', 0)} bytes")
        console.print(f"  Checksum: {info.get('checksum', 'N/A')}")
        console.print(f"  Replicas: {', '.join(info.get('replicas', []))}")
    else:
        console.print(f"[red]Error: {r.text}[/red]")
        raise typer.Exit(1)


@object_app.command("get")
def object_get(
    bucket: str,
    key: str,
    output: Path = typer.Option(..., "--output", "-o", help="Output file path"),
):
    """Download an object from a bucket."""
    r = httpx.get(f"{get_base_url()}/objects/{bucket}/{key}", timeout=30.0)
    if r.status_code == 200:
        output.write_bytes(r.content)
        console.print(f"[green]Downloaded {key} to {output}[/green]")
        console.print(f"  Size: {len(r.content)} bytes")
        checksum = r.headers.get("X-Checksum", "N/A")
        console.print(f"  Checksum: {checksum}")
    else:
        console.print(f"[red]Error: {r.text}[/red]")
        raise typer.Exit(1)


@object_app.command("delete")
def object_delete(bucket: str, key: str):
    """Delete an object from a bucket."""
    r = httpx.delete(f"{get_base_url()}/objects/{bucket}/{key}")
    if r.status_code == 200:
        console.print(f"[green]Deleted {key} from {bucket}[/green]")
    else:
        console.print(f"[red]Error: {r.text}[/red]")
        raise typer.Exit(1)


@cluster_app.command("status")
def cluster_status():
    """Show cluster status."""
    r = httpx.get(f"{get_base_url()}/cluster/status")
    data = r.json()

    console.print(f"\n[bold]Cluster Status[/bold]")
    console.print(f"  Total nodes: {data.get('total_nodes', 0)}")
    console.print(f"  Ring nodes: {data.get('ring_nodes', 0)}")
    console.print(f"  Buckets: {', '.join(data.get('buckets', [])) or 'none'}")

    table = Table(title="Nodes")
    table.add_column("ID")
    table.add_column("Address")
    table.add_column("Status")
    table.add_column("Last Seen")

    for node in data.get("nodes", []):
        status = "[green]alive[/green]" if node["alive"] else "[red]dead[/red]"
        table.add_row(node["id"], node["address"], status, node.get("last_seen", ""))

    console.print(table)


if __name__ == "__main__":
    app()
