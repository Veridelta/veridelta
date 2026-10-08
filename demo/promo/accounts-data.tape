# Made for promotional video, not the docs: the two exports, before and after the rewrite, on
# the accounts of the "From drift to rules" guide, in a font large enough to read on a phone.
# `make demo-video` renders it.
Output video/promo-accounts-data.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "head -n 6 accounts_legacy.csv"
Enter
Sleep 2s
Type "head -n 6 accounts_rewrite.csv"
Enter
Sleep 4s
