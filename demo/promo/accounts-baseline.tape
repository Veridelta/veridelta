# Made for promotional video, not the docs: a baseline accepts the purge made on purpose, and
# the defect still fails the run, on the accounts of the "From drift to rules" guide, in a
# font large enough to read on a phone. `make demo-video` renders it.
Output video/promo-accounts-baseline.mp4

Require veridelta
Source settings.tape
Set FontSize 32
Set Width 1920
Set Height 1080

Type "cat accepted.json"
Enter
Sleep 3s
Type "veridelta run -c accounts_rules.yaml --baseline accepted.json"
Enter
Sleep 5s
Type 'echo "exit code: $?"'
Enter
Sleep 4s
