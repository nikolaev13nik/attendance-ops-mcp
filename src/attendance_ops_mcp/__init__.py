from attendance_ops_mcp.server import mcp
from attendance_ops_mcp.tools import cluster, exceptions, setup


def main() -> None:
    mcp.run()
