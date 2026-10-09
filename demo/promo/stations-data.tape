# Made for promotional video, not the docs: the first rows of a weather station file as a
# legacy IDL pipeline wrote it and as its Python port writes it, made up by demo/stations.py,
# in a font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-stations-data.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "head -n 5 stations_idl.csv"
Enter
Sleep 3s
Type "head -n 5 stations_python.csv"
Enter
Sleep 4s
