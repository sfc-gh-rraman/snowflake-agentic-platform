"""
Agentic Platform Orchestrator UI
Streamlit application for visualizing and controlling agent execution.
"""

import os

import streamlit as st

st.set_page_config(
    page_title="Agentic Platform Orchestrator",
    page_icon="🤖",
    layout="wide",
)


def get_connection():
    import snowflake.connector

    if os.path.exists("/snowflake/session/token"):
        with open("/snowflake/session/token") as f:
            token = f.read()
        return snowflake.connector.connect(
            host=os.environ.get("SNOWFLAKE_HOST"),
            account=os.environ.get("SNOWFLAKE_ACCOUNT"),
            authenticator="oauth",
            token=token,
        )
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "demo")
    )


def execute_query(sql):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        if cursor.description:
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]
        return []
    finally:
        cursor.close()
        conn.close()


def get_active_plans():
    sql = """
        SELECT plan_id, use_case_description, status, approval_status, created_at
        FROM AGENTIC_PLATFORM.STATE.AGENT_EXECUTION_PLANS
        ORDER BY created_at DESC
        LIMIT 20
    """
    try:
        return execute_query(sql)
    except Exception:
        return []


def get_plan_phases(plan_id):
    sql = f"""
        SELECT phase_id, phase_name, phase_order, status, started_at, completed_at
        FROM AGENTIC_PLATFORM.STATE.AGENT_PHASE_STATE
        WHERE plan_id = '{plan_id}'
        ORDER BY phase_order
    """
    try:
        return execute_query(sql)
    except Exception:
        return []


def approve_plan(plan_id):
    sql = f"""
        UPDATE AGENTIC_PLATFORM.STATE.AGENT_EXECUTION_PLANS
        SET approval_status = 'approved', status = 'approved',
            approved_at = CURRENT_TIMESTAMP()
        WHERE plan_id = '{plan_id}'
    """
    try:
        execute_query(sql)
        return True
    except Exception:
        return False


def reject_plan(plan_id):
    sql = f"""
        UPDATE AGENTIC_PLATFORM.STATE.AGENT_EXECUTION_PLANS
        SET approval_status = 'rejected', status = 'cancelled'
        WHERE plan_id = '{plan_id}'
    """
    try:
        execute_query(sql)
        return True
    except Exception:
        return False


st.title("🤖 Agentic Platform Orchestrator")

tab1, tab2, tab3, tab4 = st.tabs(
    ["📋 Execution Plans", "🔄 Pipeline Status", "📊 Artifacts", "📈 Observability"]
)

with tab1:
    st.subheader("Execution Plans")

    col1, col2 = st.columns([2, 1])

    with col1:
        with st.expander("➕ Create New Plan", expanded=False):
            use_case = st.text_area("Describe your use case", height=100)
            data_locations = st.text_input("Data locations (comma-separated)", "@RAW.DATA_STAGE")

            if st.button("Generate Plan", type="primary"):
                st.info("Plan generation would invoke meta-agent...")
                st.session_state["pending_plan"] = {
                    "use_case": use_case,
                    "data_locations": data_locations.split(","),
                }

    plans = get_active_plans()

    if plans:
        for plan in plans:
            status_color = {
                "pending": "🟡",
                "approved": "🟢",
                "running": "🔵",
                "completed": "✅",
                "failed": "🔴",
                "cancelled": "⚫",
            }.get(plan.get("STATUS", "").lower(), "⚪")

            with st.expander(
                f"{status_color} {plan.get('USE_CASE_DESCRIPTION', 'No description')[:80]}...",
                expanded=False,
            ):
                col1, col2, col3 = st.columns(3)
                col1.metric("Status", plan.get("STATUS", "unknown"))
                col2.metric("Approval", plan.get("APPROVAL_STATUS", "pending"))
                col3.metric("Created", str(plan.get("CREATED_AT", ""))[:19])

                if plan.get("APPROVAL_STATUS", "").lower() == "pending":
                    col1, col2 = st.columns(2)
                    if col1.button("✅ Approve", key=f"approve_{plan.get('PLAN_ID')}"):
                        if approve_plan(plan.get("PLAN_ID")):
                            st.success("Plan approved!")
                            st.rerun()
                    if col2.button("❌ Reject", key=f"reject_{plan.get('PLAN_ID')}"):
                        if reject_plan(plan.get("PLAN_ID")):
                            st.warning("Plan rejected")
                            st.rerun()

                phases = get_plan_phases(plan.get("PLAN_ID"))
                if phases:
                    st.write("**Phases:**")
                    for phase in phases:
                        phase_status = {
                            "pending": "⏳",
                            "running": "🔄",
                            "completed": "✅",
                            "failed": "❌",
                            "skipped": "⏭️",
                        }.get(phase.get("STATUS", "").lower(), "⚪")
                        st.write(
                            f"{phase_status} {phase.get('PHASE_ORDER', 0)}. {phase.get('PHASE_NAME', 'Unknown')}"
                        )
    else:
        st.info("No execution plans found. Create one above!")

with tab2:
    st.subheader("Pipeline Status")

    col1, col2 = st.columns(2)

    with col1:
        st.write("**Agent Status**")
        agents = [
            ("Parquet Processor", "🟢 Ready"),
            ("Document Chunker", "🟢 Ready"),
            ("ML Model Builder", "🟢 Ready"),
            ("Cortex Search Builder", "🟢 Ready"),
            ("Semantic Model Generator", "🟢 Ready"),
            ("App Code Generator", "🟢 Ready"),
            ("SPCS Deployer", "🟢 Ready"),
        ]
        for agent, status in agents:
            st.write(f"• {agent}: {status}")

    with col2:
        st.write("**System Health**")
        st.metric("Active Executions", "0")
        st.metric("Queue Depth", "0")
        st.metric("Avg Latency", "N/A")

with tab3:
    st.subheader("Artifacts")

    artifact_sql = """
        SELECT artifact_type, COUNT(*) as count
        FROM AGENTIC_PLATFORM.STATE.AGENT_ARTIFACTS
        WHERE status = 'active'
        GROUP BY artifact_type
        ORDER BY count DESC
    """

    try:
        artifacts = execute_query(artifact_sql)
        if artifacts:
            import pandas as pd

            df = pd.DataFrame(artifacts)
            st.bar_chart(df.set_index("ARTIFACT_TYPE")["COUNT"])

            st.dataframe(df, use_container_width=True)
        else:
            st.info("No artifacts registered yet.")
    except Exception as e:
        st.warning(f"Could not load artifacts: {e}")

with tab4:
    st.subheader("Cortex Usage")

    usage_sql = """
        SELECT
            DATE_TRUNC('hour', created_at) as hour,
            call_type,
            COUNT(*) as calls,
            AVG(latency_ms) as avg_latency
        FROM AGENTIC_PLATFORM.STATE.CORTEX_CALL_LOGS
        WHERE created_at > DATEADD(day, -7, CURRENT_TIMESTAMP())
        GROUP BY 1, 2
        ORDER BY 1 DESC
    """

    try:
        usage = execute_query(usage_sql)
        if usage:
            import pandas as pd

            df = pd.DataFrame(usage)
            st.line_chart(df.pivot(index="HOUR", columns="CALL_TYPE", values="CALLS"))

            st.dataframe(df, use_container_width=True)
        else:
            st.info("No Cortex call logs yet.")
    except Exception as e:
        st.warning(f"Could not load usage data: {e}")


st.sidebar.title("Configuration")
st.sidebar.selectbox("Database", ["AGENTIC_PLATFORM"])
st.sidebar.selectbox("Warehouse", ["COMPUTE_WH"])

if st.sidebar.button("🔄 Refresh"):
    st.rerun()

st.sidebar.markdown("---")
st.sidebar.markdown("**Agentic Platform v1.0**")
st.sidebar.markdown("Powered by Snowflake Cortex")
