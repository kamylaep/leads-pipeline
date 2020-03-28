package br.com.dr.leads.test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class FileHelper {

  private FileHelper() {
  }

  public static List<String> readOutput(String path) throws IOException {
    return Files.walk(Paths.get(path))
        .filter(p -> p.toFile().isFile())
        .flatMap(FileHelper::readFileToStream)
        .collect(Collectors.toList());
  }

  public static Stream<? extends String> readFileToStream(Path p) {
    try {
      return Files.readAllLines(p).stream();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static long getNumberOfShardsProduced(String path) throws IOException {
    return Files.walk(Paths.get(path))
        .filter(p -> p.toFile().isFile())
        .count();
  }


}
