"""Snowflake Agentic Platform CLI.

A zero-to-one platform for transforming use case descriptions + raw data
into deployed AI applications on Snowflake.

Usage:
    agentic-platform run "Your use case description" --data @STAGE/path
    agentic-platform status <plan_id>
    agentic-platform agents list
    agentic-platform setup
"""

import os

import typer
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table

app = typer.Typer(
    name="agentic-platform",
    help="Zero-to-One Agentic Data Platform - From use case to deployed AI app",
    no_args_is_help=True,
)

console = Console()


def get_connection_name() -> str:
    """Get the Snowflake connection name from env or default."""
    return os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")


@app.command("run")
def run_pipeline(
    use_case: str = typer.Argument(..., help="Natural language description of your use case"),
    data: str | None = typer.Option(
        None, "--data", "-d", help="Stage path to your data (e.g., @RAW.DATA_STAGE)"
    ),
    auto_approve: bool = typer.Option(False, "--auto-approve", "-y", help="Skip approval step"),
    database: str = typer.Option("AGENTIC_PLATFORM", "--database", "-db", help="Target database"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Run the agentic platform to build an AI application from your use case."""
    conn_name = connection or get_connection_name()
    os.environ["SNOWFLAKE_CONNECTION_NAME"] = conn_name

    console.print(
        Panel.fit(
            f"[bold blue]Snowflake Agentic Platform[/bold blue]\n"
            f"Use Case: {use_case[:100]}...\n"
            f"Database: {database}\n"
            f"Connection: {conn_name}",
            title="Starting Pipeline",
        )
    )

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("Parsing use case...", total=None)

        try:
            from src.meta_agent.graph import MetaAgentState, create_meta_agent_graph

            graph = create_meta_agent_graph()

            initial_state: MetaAgentState = {
                "use_case_description": use_case,
                "data_locations": [data] if data else [],
                "data_assets": [],
                "parsed_requirements": None,
                "data_profile": None,
                "available_agents": [],
                "execution_plan": None,
                "approval_status": "approved" if auto_approve else "pending",
                "approval_feedback": None,
                "current_phase": "start",
                "error": None,
                "messages": [],
            }

            progress.update(task, description="Generating execution plan...")
            result = graph.invoke(initial_state)

            plan_id = result.get("execution_plan", {}).get("plan_id", "N/A")

            console.print("\n[green]✓ Pipeline initiated![/green]")
            console.print(f"Plan ID: [bold]{plan_id}[/bold]")

            if not auto_approve:
                console.print("\n[yellow]Waiting for approval. Run:[/yellow]")
                console.print(f"  agentic-platform approve {plan_id}")

        except ImportError:
            console.print(
                "[yellow]Meta-agent not fully implemented. Running in demo mode.[/yellow]"
            )
            import uuid

            plan_id = str(uuid.uuid4())
            console.print("\n[green]✓ Demo plan created![/green]")
            console.print(f"Plan ID: [bold]{plan_id}[/bold]")
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            raise typer.Exit(1)


@app.command("status")
def check_status(
    plan_id: str = typer.Argument(..., help="Execution plan ID"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Check the status of an execution plan."""
    conn_name = connection or get_connection_name()

    try:
        import snowflake.connector

        conn = snowflake.connector.connect(connection_name=conn_name)
        cursor = conn.cursor()

        cursor.execute(f"""
            SELECT status, approval_status, error_message, created_at, completed_at
            FROM AGENTIC_PLATFORM.STATE.AGENT_EXECUTION_PLANS
            WHERE plan_id = '{plan_id}'
        """)

        row = cursor.fetchone()

        if row:
            status, approval, error, created, completed = row

            table = Table(title=f"Plan Status: {plan_id[:8]}...")
            table.add_column("Field", style="cyan")
            table.add_column("Value", style="white")

            table.add_row("Status", status)
            table.add_row("Approval", approval)
            table.add_row("Created", str(created))
            table.add_row("Completed", str(completed) if completed else "In Progress")
            if error:
                table.add_row("Error", error[:100])

            console.print(table)
        else:
            console.print(f"[yellow]Plan {plan_id} not found[/yellow]")

        cursor.close()
        conn.close()

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("approve")
def approve_plan(
    plan_id: str = typer.Argument(..., help="Execution plan ID to approve"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Approve an execution plan to begin execution."""
    conn_name = connection or get_connection_name()

    try:
        import snowflake.connector

        conn = snowflake.connector.connect(connection_name=conn_name)
        cursor = conn.cursor()

        cursor.execute(f"""
            UPDATE AGENTIC_PLATFORM.STATE.AGENT_EXECUTION_PLANS
            SET approval_status = 'approved',
                status = 'running',
                approved_at = CURRENT_TIMESTAMP(),
                approved_by = CURRENT_USER()
            WHERE plan_id = '{plan_id}'
            AND approval_status = 'pending'
        """)

        if cursor.rowcount > 0:
            console.print(f"[green]✓ Plan {plan_id[:8]}... approved![/green]")
            console.print("Execution will begin shortly.")
        else:
            console.print("[yellow]Plan not found or already approved[/yellow]")

        cursor.close()
        conn.close()

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("agents")
def list_agents(
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """List available agents in the registry."""
    connection or get_connection_name()

    agents = [
        ("parquet_processor", "1.0.0", "preprocessing", "Process Parquet files from stage"),
        ("document_chunker", "1.0.0", "preprocessing", "Extract and chunk documents (PDF, etc.)"),
        ("ml_model_builder", "1.0.0", "ml", "Train and register ML models"),
        ("feature_store_builder", "1.0.0", "ml", "Automated feature engineering"),
        ("cortex_search_builder", "1.0.0", "cortex", "Create Cortex Search services"),
        ("semantic_model_generator", "1.0.0", "cortex", "Generate semantic models for Analyst"),
        ("app_code_generator", "1.0.0", "app", "Generate Streamlit/React applications"),
        ("spcs_deployer", "1.0.0", "deployment", "Deploy to Snowpark Container Services"),
        ("validation_agent", "1.0.0", "quality", "Data quality and validation checks"),
        ("improvement_agent", "1.0.0", "maintenance", "Handle user improvement requests"),
    ]

    table = Table(title="Available Agents")
    table.add_column("Agent", style="cyan", no_wrap=True)
    table.add_column("Version", style="white")
    table.add_column("Category", style="green")
    table.add_column("Description", style="white")

    for agent in agents:
        table.add_row(*agent)

    console.print(table)


@app.command("setup")
def setup_database(
    database: str = typer.Option("AGENTIC_PLATFORM", "--database", "-db", help="Database name"),
    warehouse: str = typer.Option("COMPUTE_WH", "--warehouse", "-wh", help="Warehouse name"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Set up the required database schemas and tables."""
    conn_name = connection or get_connection_name()

    console.print(f"[bold]Setting up {database}...[/bold]")

    schemas = ["RAW", "CURATED", "ML", "DOCS", "ANALYTICS", "CORTEX", "ORCHESTRATOR", "STATE"]

    try:
        import snowflake.connector

        conn = snowflake.connector.connect(connection_name=conn_name)
        cursor = conn.cursor()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Creating database...", total=len(schemas) + 2)

            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
            progress.advance(task)

            cursor.execute(f"USE DATABASE {database}")

            for schema in schemas:
                progress.update(task, description=f"Creating schema {schema}...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
                progress.advance(task)

            progress.update(task, description="Creating state tables...")

            cursor.execute("USE SCHEMA STATE")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS AGENT_EXECUTION_PLANS (
                    plan_id VARCHAR(36) DEFAULT UUID_STRING() PRIMARY KEY,
                    use_case_description TEXT NOT NULL,
                    parsed_requirements VARIANT,
                    execution_plan VARIANT,
                    status VARCHAR(20) DEFAULT 'pending',
                    approval_status VARCHAR(20) DEFAULT 'pending',
                    approved_by VARCHAR(100),
                    approved_at TIMESTAMP_NTZ,
                    error_message TEXT,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    completed_at TIMESTAMP_NTZ
                )
            """)
            progress.advance(task)

        cursor.close()
        conn.close()

        console.print(f"[green]✓ Database {database} setup complete![/green]")
        console.print(f"Schemas created: {', '.join(schemas)}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("feature-store")
def run_feature_store(
    source_table: str = typer.Argument(..., help="Source table for feature engineering"),
    target_column: str | None = typer.Option(
        None, "--target", "-t", help="Target column for ML task"
    ),
    ml_task: str | None = typer.Option(
        None, "--task", help="ML task type (classification, regression, etc.)"
    ),
    database: str = typer.Option("AGENTIC_PLATFORM", "--database", "-db", help="Database name"),
    schema: str = typer.Option("ML", "--schema", "-s", help="Schema name"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Run the feature store pipeline on a source table."""
    conn_name = connection or get_connection_name()
    os.environ["SNOWFLAKE_CONNECTION_NAME"] = conn_name

    console.print("[bold]Running Feature Store Pipeline[/bold]")
    console.print(f"Source: {source_table}")

    try:
        from src.agents.ml.feature_store_graph import run_feature_store_pipeline

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Processing...", total=None)

            result = run_feature_store_pipeline(
                source_table=source_table,
                database=database,
                schema=schema,
                target_column=target_column,
                ml_task=ml_task,
            )

            progress.update(task, description="Complete!")

        feature_table = result.get("feature_table")
        engineered = len(result.get("engineered_features", []))

        console.print("\n[green]✓ Feature engineering complete![/green]")
        console.print(f"Engineered {engineered} features")
        if feature_table:
            console.print(f"Feature table: {feature_table}")

        if result.get("errors"):
            console.print("\n[yellow]Warnings:[/yellow]")
            for err in result["errors"]:
                console.print(f"  - {err}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("search")
def create_search_service(
    source_table: str = typer.Argument(..., help="Source table with text data"),
    service_name: str = typer.Argument(..., help="Name for the search service"),
    search_column: str | None = typer.Option(None, "--column", help="Column to search on"),
    database: str = typer.Option("AGENTIC_PLATFORM", "--database", "-db", help="Database name"),
    schema: str = typer.Option("CORTEX", "--schema", "-s", help="Schema name"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Create a Cortex Search service from a table."""
    conn_name = connection or get_connection_name()
    os.environ["SNOWFLAKE_CONNECTION_NAME"] = conn_name

    console.print("[bold]Creating Cortex Search Service[/bold]")
    console.print(f"Source: {source_table}")
    console.print(f"Service: {service_name}")

    try:
        from src.agents.search.search_graph import run_search_pipeline

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Creating search service...", total=None)

            result = run_search_pipeline(
                source_table=source_table,
                service_name=service_name,
                database=database,
                schema=schema,
                search_column=search_column,
            )

            progress.update(task, description="Complete!")

        service = result.get("search_service")

        if service:
            console.print("\n[green]✓ Search service created![/green]")
            console.print(f"Service: {service}")
        else:
            console.print("\n[yellow]Service creation may have failed[/yellow]")

        if result.get("errors"):
            console.print("\n[red]Errors:[/red]")
            for err in result["errors"]:
                console.print(f"  - {err}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("semantic")
def create_semantic_model(
    source_table: str = typer.Argument(..., help="Source table for semantic model"),
    model_name: str = typer.Argument(..., help="Name for the semantic model"),
    context: str = typer.Option(
        "General analytics", "--context", help="Business context description"
    ),
    database: str = typer.Option("AGENTIC_PLATFORM", "--database", "-db", help="Database name"),
    schema: str = typer.Option("ANALYTICS", "--schema", "-s", help="Schema name"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Create a semantic model YAML from a table."""
    conn_name = connection or get_connection_name()
    os.environ["SNOWFLAKE_CONNECTION_NAME"] = conn_name

    console.print("[bold]Creating Semantic Model[/bold]")
    console.print(f"Source: {source_table}")
    console.print(f"Model: {model_name}")

    try:
        from src.agents.semantic.semantic_graph import run_semantic_pipeline

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Generating semantic model...", total=None)

            result = run_semantic_pipeline(
                source_table=source_table,
                model_name=model_name,
                database=database,
                schema=schema,
                business_context=context,
            )

            progress.update(task, description="Complete!")

        yaml_content = result.get("yaml_content")
        dims = len(result.get("dimensions", []))
        facts = len(result.get("facts", []))
        vqs = len(result.get("verified_queries", []))

        console.print("\n[green]✓ Semantic model generated![/green]")
        console.print(f"Dimensions: {dims}, Facts: {facts}, Verified Queries: {vqs}")

        if yaml_content:
            console.print("\n[bold]YAML Preview:[/bold]")
            console.print(yaml_content[:500] + "...")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("improve")
def run_improvement(
    request: str = typer.Argument(..., help="Improvement request in natural language"),
    plan_id: str | None = typer.Option(None, "--plan", "-p", help="Plan ID to improve"),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
):
    """Submit an improvement request for an existing application."""
    conn_name = connection or get_connection_name()
    os.environ["SNOWFLAKE_CONNECTION_NAME"] = conn_name

    import uuid

    plan_id = plan_id or str(uuid.uuid4())

    console.print("[bold]Processing Improvement Request[/bold]")
    console.print(f"Request: {request[:100]}...")

    try:
        from src.agents.improvement.improvement_graph import run_improvement_pipeline

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Analyzing request...", total=None)

            result = run_improvement_pipeline(
                request=request,
                plan_id=plan_id,
            )

            progress.update(task, description="Complete!")

        notification = result.get("notification", "")
        request_type = result.get("request_type", "unknown")

        console.print("\n[green]✓ Improvement processed![/green]")
        console.print(f"Type: {request_type}")

        if notification:
            console.print(f"\n{notification}")

    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)


@app.command("orchestrator")
def deploy_orchestrator(
    database: str = typer.Option("AGENTIC_PLATFORM", "--database", "-db", help="Target database"),
    schema: str = typer.Option("ORCHESTRATOR", "--schema", "-s", help="Target schema"),
    compute_pool: str = typer.Option(
        "AGENTIC_COMPUTE_POOL", "--pool", "-p", help="SPCS compute pool"
    ),
    connection: str | None = typer.Option(
        None, "--connection", "-c", help="Snowflake connection name"
    ),
    local: bool = typer.Option(False, "--local", "-l", help="Run locally instead of SPCS"),
):
    """Deploy the orchestrator UI to Snowpark Container Services.

    The orchestrator provides a visual workflow UI for monitoring
    LangGraph execution with Langfuse tracing integration.

    Examples:
        # Deploy to SPCS (recommended)
        agentic-platform orchestrator

        # Run locally for development
        agentic-platform orchestrator --local
    """
    conn_name = connection or get_connection_name()
    os.environ["SNOWFLAKE_CONNECTION_NAME"] = conn_name

    if local:
        console.print("[bold]Starting Orchestrator Locally[/bold]")
        console.print("Backend: http://localhost:8000")
        console.print("Frontend: http://localhost:5173")
        console.print("\nTo start manually:")
        console.print("  cd orchestrator/backend && uvicorn server:app --reload")
        console.print("  cd orchestrator/frontend && npm run dev")
        return

    console.print(
        Panel.fit(
            f"[bold blue]Deploying Orchestrator to SPCS[/bold blue]\n\n"
            f"Database: {database}\n"
            f"Schema: {schema}\n"
            f"Compute Pool: {compute_pool}\n"
            f"Connection: {conn_name}\n\n"
            f"[yellow]This will:[/yellow]\n"
            f"  1. Build Docker image\n"
            f"  2. Push to Snowflake registry\n"
            f"  3. Create SPCS service\n\n"
            f"[cyan]Monitoring: Langfuse Cloud[/cyan]",
            title="SPCS Deployment",
        )
    )

    import subprocess
    from pathlib import Path

    script_path = Path(__file__).parent.parent.parent / "orchestrator" / "deploy" / "deploy.sh"

    if not script_path.exists():
        console.print(f"[red]Deploy script not found at {script_path}[/red]")
        raise typer.Exit(1)

    env = os.environ.copy()
    env["DATABASE"] = database
    env["SCHEMA"] = schema
    env["COMPUTE_POOL"] = compute_pool
    env["CONNECTION"] = conn_name

    console.print("\n[bold]Running deployment script...[/bold]\n")

    try:
        result = subprocess.run(
            ["bash", str(script_path)],
            env=env,
            cwd=script_path.parent.parent,
            check=True,
        )
        if result.returncode == 0:
            console.print("\n[green]✓ Orchestrator deployed successfully![/green]")
        else:
            console.print("\n[red]Deployment failed[/red]")
            raise typer.Exit(1)
    except subprocess.CalledProcessError as e:
        console.print(f"\n[red]Deployment failed: {e}[/red]")
        raise typer.Exit(1)
    except FileNotFoundError:
        console.print("[red]bash not found. Please install bash or run deploy.sh manually.[/red]")
        raise typer.Exit(1)


@app.command("ui")
def start_ui(
    port: int = typer.Option(8000, "--port", "-p", help="Backend port"),
    frontend_port: int = typer.Option(5173, "--frontend-port", "-fp", help="Frontend port"),
):
    """Start the orchestrator UI locally for development.

    This starts both the FastAPI backend and React frontend in development mode.
    Use this for local development and testing before deploying to SPCS.

    The UI provides:
      - Visual workflow DAG showing agent execution
      - Real-time logs via WebSocket
      - Langfuse integration for LLM tracing
    """
    from pathlib import Path

    backend_dir = Path(__file__).parent.parent.parent / "orchestrator" / "backend"
    frontend_dir = Path(__file__).parent.parent.parent / "orchestrator" / "frontend"

    if not backend_dir.exists():
        console.print(f"[red]Backend directory not found: {backend_dir}[/red]")
        raise typer.Exit(1)

    console.print(
        Panel.fit(
            f"[bold blue]Starting Orchestrator UI[/bold blue]\n\n"
            f"Backend: http://localhost:{port}\n"
            f"Frontend: http://localhost:{frontend_port}\n"
            f"API Docs: http://localhost:{port}/docs\n\n"
            f"[yellow]Press Ctrl+C to stop[/yellow]",
            title="Local Development",
        )
    )

    console.print("\n[bold]Starting backend...[/bold]")
    console.print(f"  cd {backend_dir}")
    console.print(f"  uvicorn server:app --port {port} --reload\n")

    console.print("[bold]Starting frontend...[/bold]")
    console.print(f"  cd {frontend_dir}")
    console.print(f"  npm run dev -- --port {frontend_port}\n")

    console.print("[yellow]Run these commands in separate terminals.[/yellow]")


if __name__ == "__main__":
    app()
