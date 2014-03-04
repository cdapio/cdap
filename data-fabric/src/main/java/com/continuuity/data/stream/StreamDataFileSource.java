/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 *
 */
final class StreamDataFileSource {

  private final int bucketId;
  private final int bucketSequence;
  private final StreamDataFileReader reader;

  StreamDataFileSource(int bucketId, int bucketSequence, StreamDataFileReader reader) {
    this.bucketId = bucketId;
    this.bucketSequence = bucketSequence;
    this.reader = reader;
  }

  int getBucketId() {
    return bucketId;
  }

  int getBucketSequence() {
    return bucketSequence;
  }

  StreamDataFileReader getReader() {
    return reader;
  }
}
