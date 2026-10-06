# The quick start, as the README shows it. `make demo` renders this tape with vhs.
Output ../docs/assets/demo.gif

Set Shell "bash"
Set FontSize 16
Set Width 900
Set Height 800
Set TypingSpeed 50ms

Type "cat veridelta.yaml"
Enter
Sleep 2s

Type "veridelta validate -c veridelta.yaml"
Enter
Sleep 2.5s

Type "veridelta run -c veridelta.yaml"
Enter
Sleep 4s

Type "echo $?"
Enter
Sleep 3s
