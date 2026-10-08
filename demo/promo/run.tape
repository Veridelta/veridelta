# Made for promotional video, not the docs: the quick start's run and exit code, in a
# font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-run.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 700
Set Height 1080

Type "veridelta run -q"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
