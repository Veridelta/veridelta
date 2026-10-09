# Made for promotional video, not the docs: a baseline accepts the pressure fix made on purpose,
# and the port's own bug still fails the run, in a font large enough to read on a phone.
# `make demo-video` renders it.
Output video/promo-stations-baseline.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "head -n 16 stations_accepted.json"
Enter
Sleep 3s
Type "veridelta run -c stations_rules.yaml --baseline stations_accepted.json"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
