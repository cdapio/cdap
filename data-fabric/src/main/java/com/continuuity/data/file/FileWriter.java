/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.file;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 * This interface represents classes that can write data to file.
 *
 * @param <T> Type of data that can be written to the file.
 */
public interface FileWriter<T> extends Closeable, Flushable {

  void append(T event) throws IOException;
}
