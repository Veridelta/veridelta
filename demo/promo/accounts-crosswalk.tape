# Made for promotional video, not the docs: crosswalk proposes a value map for the recoded
# status, once the suggested rules are in, on the accounts of the "From drift to rules" guide,
# in a font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-accounts-crosswalk.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "veridelta crosswalk -c accounts_suggested.yaml"
Enter
Sleep 7s
