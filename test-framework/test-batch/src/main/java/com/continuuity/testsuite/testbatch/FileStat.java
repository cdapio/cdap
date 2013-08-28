package com.continuuity.testsuite.testbatch;

import java.util.ArrayList;

/**
 *  FileStat Object, stored in ObjectStore
 */
public class FileStat {
  private final String filename;
  private long wordCount;
  private final String line;

  public FileStat(String filename, String line) {
    this.filename = filename;
    this.wordCount = 0;
    this.line = line;
  }

  public String getFilename() {
    return filename;
  }

  public long getWordCount() {
    return wordCount;
  }

  public String getLine() {
    return line;
  }

  public void countLine() {
    String[] str;
    str = line.split("\\s+");
    wordCount = str.length;
  }
}
