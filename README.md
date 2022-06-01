# spring-file-parse
Comma delimiter file parsing using Java stream and flux

This is a comparison between Java Stream and Flux to parse a comma delimiter file and write the objects to two different format files.

1. Using Files.lines to create a Stream<String>, then parse each line to Map<String, Object> object from the stream. After parsing all lines, write all parsed objects to files one by one.
  
2. Using Files.lines to create a Flux<String>, then parse each line to Map<String, Object> object and write the object incremently to two files at the same time.
  
3. Using Files.lines to create a Flux<String>, then parse each line to Map<String, Object> object and write the object incremently to the files (with different threads).

  <TABLE>
  <TR><TD>1</TD></TR>
  </TABLE>
