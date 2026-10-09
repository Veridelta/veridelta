# Made for promotional video, not the docs: the weather station rules, and the run they leave,
# the fix made on purpose and the port's own bug, in a font large enough to read on a phone.
# `make demo-video` renders it.
Output video/promo-stations-rules.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "cat stations_rules.yaml"
Enter
Sleep 3s
Type "veridelta run -c stations_rules.yaml"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
