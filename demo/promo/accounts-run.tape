# Made for promotional video, not the docs: the first run, on two files and their key, with no
# configuration file, on the accounts of the "From drift to rules" guide, in a font large
# enough to read on a phone. `make demo-video` renders it.
Output video/promo-accounts-run.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "veridelta run accounts_legacy.csv accounts_rewrite.csv --key account_id"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
