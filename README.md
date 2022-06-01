# spring-file-parse
Comma delimiter file parsing using Java stream and flux

This is a comparison between Java Stream and Flux to parse a comma delimiter file and write the objects to two different format files.

1. Using Files.lines to create a Stream<String>, then parse each line to Map<String, Object> object from the stream. After parsing all lines, write all parsed objects to files one by one.
  
2. Using Files.lines to create a Flux<String>, then parse each line to Map<String, Object> object and write the object incremently to two files at the same time. [ single subscriber to Flux]
  
3. Using Files.lines to create a Flux<String>, then parse each line to Map<String, Object> object and write the object incremently to the files (with different threads). [multiple subscribers to Flux]

  The following is the sample size based on a million lines file.
  <TABLE>
    <TR><TD></TD><TD>Memeory usage</TD><TD>Total Time</TD></TR>
  <TR><TD>1</TD><TD>1165MB</TD><TD>48236ms</TD></TR>
  <TR><TD>2</TD><TD>12MB</TD><TD>46910ms</TD></TR>
  <TR><TD>3</TD><TD>12MB</TD><TD>47663ms</TD></TR>
  </TABLE>
