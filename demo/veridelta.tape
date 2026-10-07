# The quick start, as the README shows it. `make demo` renders this tape with vhs.
Output ../docs/assets/demo.gif
Require veridelta
Source settings.tape
Set Height 1000

Type "cat veridelta.yaml"
Enter
Sleep 2s

Type "cat legacy.csv"
Enter
Sleep 2s

Type "cat modern.csv"
Enter
Sleep 2.5s

Type "veridelta validate -c veridelta.yaml"
Enter
Sleep 2.5s

Type "veridelta run -c veridelta.yaml"
Enter
Sleep 4s

Type 'echo "exit code for CI: $? (0 match, 1 drift, 3 error)"'
Enter
Sleep 3s
