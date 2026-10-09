# Made for promotional video, not the docs: the first run on the weather station files, on two
# files and their key, with no configuration file, in a font large enough to read on a phone.
# `make demo-video` renders it.
Output video/promo-stations-run.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "veridelta run stations_idl.csv stations_python.csv --key station --key obs_date"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
