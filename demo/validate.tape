# `veridelta validate` catches a wrong primary key before a run reads a row of data.
Output ../docs/assets/demo-validate.gif
Require veridelta
Source settings.tape
Set Height 440

Type "cat orders.yaml"
Enter
Sleep 2s

Type "head -n 2 legacy.csv"
Enter
Sleep 2s

Type "veridelta validate -c orders.yaml --schemas"
Enter
Sleep 4s

Type 'echo "exit code: $?"'
Enter
Sleep 3s
