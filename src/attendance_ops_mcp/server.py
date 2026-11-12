from mcp.server.mcpserver import MCPServer

mcp = MCPServer(
    name="attendance-ops",
    instructions=(
        "Read-only inspection of the Attendance stack: pods, logs, Postgres and Kafka. "
        "Every tool takes env: 'dev' (local Rancher Desktop) or 'prod' (live GKE). "
        "Pass env only when the user explicitly named the environment; otherwise leave it out "
        "and the server will ask the user. If a tool reports a setup problem, call check_setup."
    ),
)
