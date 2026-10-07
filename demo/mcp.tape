# A script calls the tools of `veridelta mcp` as an agent's host would, over stdio.
Output ../docs/assets/demo-mcp.gif
Require veridelta
Source settings.tape
Set Height 820

Type "# A client calls the MCP server's tools, as an agent's host does."
Enter
Sleep 1.5s

Type "python mcp_client.py"
Enter
Sleep 7s
