package br.com.dr.leads.helper;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class FileNaming implements Write.FileNaming {

  private static final DateTimeFormatter DIR_NAME_FORMATTER = DateTimeFormat.forPattern("yyyy/MM/dd/HH/");
  private static final DateTimeFormatter FILE_NAME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH-mm-ss");

  @Override
  public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
    IntervalWindow intervalWindow = (IntervalWindow) window;

    String dirName = DIR_NAME_FORMATTER.print(intervalWindow.start());
    String fileName = String.format("%s-of-%s_from-%s-to-%s", shardIndex, numShards, FILE_NAME_FORMATTER.print(intervalWindow.start()), FILE_NAME_FORMATTER.print(intervalWindow.end()));
    return dirName + fileName;
  }
}

