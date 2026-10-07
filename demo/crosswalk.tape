# `veridelta crosswalk` proposes a value map from the data, with the evidence for each entry.
Output ../docs/assets/demo-crosswalk.gif
Require veridelta
Source settings.tape
Set Height 580

Type "head -n 4 crm.csv"
Enter
Sleep 2s

Type "head -n 4 warehouse.csv"
Enter
Sleep 2s

Type "veridelta crosswalk -c crosswalk.yaml"
Enter
Sleep 5s
