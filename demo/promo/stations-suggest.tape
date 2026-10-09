# Made for promotional video, not the docs: the rename declared, then suggest explains the
# rest of the weather station noise with its evidence, in a font large enough to read on a
# phone. `make demo-video` renders it.
Output video/promo-stations-suggest.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "cat stations.yaml"
Enter
Sleep 3s
Type "veridelta suggest -c stations.yaml"
Enter
Sleep 6s
