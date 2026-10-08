# Made for promotional video, not the docs: a script calls the MCP server's tools as an
# agent's host does, in a font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-mcp.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 900
Set Height 1080

Type "# A script stands in for an agent."
Enter
Sleep 1.5s
Type "python mcp_client.py --brief"
Enter
Sleep 9s
