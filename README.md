# spring-file-parse
Comma delimiter file parsing using Java stream and flux

This is a comparison between Java Stream and Flux to parse a comma delimiter file and write the objects to file.

1. Using Files.lines to create a Stream<String>, then parse each line to Map<String, Object> object from the stream. After parsing all lines, write all parsed objects to a single file.
  
2. Using Files.lines to create a Flux<String>, then parse each line to Map<String, Object> object and write the object incremently to the file.
  
3. Using Files.lines to create a Flux<String>, then parse each line to Map<String, Object> object and write the object incremently to two different files (with different format).
