/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 * An enum to distinguish different file types in stream data file.
 */
public enum StreamFileType {

  EVENT("dat"),
  INDEX("idx");

  private final String suffix;

  private StreamFileType(String suffix) {
    this.suffix = suffix;
  }

  public boolean isMatched(String fileName) {
    return fileName.endsWith("." + suffix);
  }

  public String getSuffix() {
    return suffix;
  }

  public static boolean isMatched(String fileName, StreamFileType type) {
    return type.isMatched(fileName);
  }

  public static StreamFileType getType(String fileName) {
    for (StreamFileType type : StreamFileType.values()) {
      if (type.isMatched(fileName)) {
        return type;
      }
    }

    throw new IllegalArgumentException("Unknown stream file type for " + fileName);
  }
}
