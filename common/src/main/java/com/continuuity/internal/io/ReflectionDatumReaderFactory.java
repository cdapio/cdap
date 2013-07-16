/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.io;

import com.google.common.reflect.TypeToken;

/**
 * A {@link DatumReaderFactory} that creates {@link ReflectionDatumReader}.
 */
public final class ReflectionDatumReaderFactory implements DatumReaderFactory {

  @Override
  public <T> DatumReader<T> create(TypeToken<T> type, Schema schema) {
    return new ReflectionDatumReader<T>(schema, type);
  }
}
