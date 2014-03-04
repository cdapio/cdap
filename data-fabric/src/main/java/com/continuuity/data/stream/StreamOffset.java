/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

/**
 *
 */
public interface StreamOffset {

  int getBucketId();

  int getBucketSequence();

  long getOffset();
}
