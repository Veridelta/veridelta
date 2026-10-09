# Made for promotional video, not the docs: the fixed port passes, with the pressure fix still
# accepted, in a font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-stations-fixed.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "veridelta run -c stations_fixed.yaml --baseline stations_accepted.json"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
