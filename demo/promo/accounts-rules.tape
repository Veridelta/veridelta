# Made for promotional video, not the docs: the run with all four rules declared, on the
# accounts of the "From drift to rules" guide, in a font large enough to read on a phone.
# `make demo-video` renders it.
Output video/promo-accounts-rules.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "cat accounts_rules.yaml"
Enter
Sleep 3s
Type "veridelta run -c accounts_rules.yaml"
Enter
Sleep 6s
