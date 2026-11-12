from attendance_ops_mcp.server import mcp
from attendance_ops_mcp.tools import cluster, exceptions, kafka, postgres, setup


def main() -> None:
    mcp.run()
