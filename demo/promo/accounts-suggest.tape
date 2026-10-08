# Made for promotional video, not the docs: suggest proposes a rule for each kind of noise,
# with its evidence, on the accounts of the "From drift to rules" guide, in a font large
# enough to read on a phone. `make demo-video` renders it.
Output video/promo-accounts-suggest.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "cat accounts.yaml"
Enter
Sleep 2.5s
Type "veridelta suggest -c accounts.yaml"
Enter
Sleep 8s
