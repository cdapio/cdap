/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 * An enum to distinguish different file types in stream data file.
 */
enum StreamFileType {

  EVENT("dat"),
  INDEX("idx");

  private final String suffix;

  StreamFileType(String suffix) {
    this.suffix = suffix;
  }

  boolean isMatched(String fileName) {
    return fileName.endsWith("." + suffix);
  }

  String getSuffix() {
    return suffix;
  }

  static boolean isMatched(String fileName, StreamFileType type) {
    return type.isMatched(fileName);
  }

  static StreamFileType getType(String fileName) {
    for (StreamFileType type : StreamFileType.values()) {
      if (type.isMatched(fileName)) {
        return type;
      }
    }

    throw new IllegalArgumentException("Unknown stream file type for " + fileName);
  }
}
