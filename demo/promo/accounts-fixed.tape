# Made for promotional video, not the docs: the rewrite's export, with the defect fixed, passes
# against the same rules and baseline, on the accounts of the "From drift to rules" guide, in a
# font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-accounts-fixed.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "veridelta run -c accounts_fixed.yaml --baseline accepted.json"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
