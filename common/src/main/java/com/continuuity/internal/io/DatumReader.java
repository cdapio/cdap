/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.io;

import com.continuuity.common.io.Decoder;

import java.io.IOException;

/**
 * Represents reader for decoding object.
 *
 * @param <T> type T to be deserialized.
 */
public interface DatumReader<T> {

  T read(Decoder decoder, Schema sourceSchema) throws IOException;
}
