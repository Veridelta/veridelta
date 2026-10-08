# Made for promotional video, not the docs: the quick start's configuration and files,
# in a font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-data.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 700
Set Height 780

Type "cat veridelta.yaml"
Enter
Sleep 2.5s
Type "cat legacy.csv"
Enter
Sleep 2.5s
Type "cat modern.csv"
Enter
Sleep 4s
