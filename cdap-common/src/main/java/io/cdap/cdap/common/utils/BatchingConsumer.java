/*
 * Copyright © 2018-2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */


package io.cdap.cdap.common.utils;

import com.google.common.base.Throwables;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Represents an operation that accepts a single input argument and returns no result. Like the
 * Consumer, Batching Consumer is expected to operate via side-effects. This implements
 * AutoCloseable so it must be closed. This class is not thread-safe.
 *
 * This is a functional interface whose functional method is accept(Object). It accepts a batch of
 * objects and then calls the accept method of a child consumer which accepts a list of Objects.
 * Type parameters:
 *
 * @param <T> – the type of the input to the operation
 */
public class BatchingConsumer<T> implements Consumer<T>, AutoCloseable {

  private List<T> buffer;
  private final int batchSize;
  private final Consumer<List<T>> child;

  /**
   * Constructs an instance of the BatchingConsumer
   *
   * @param child - the child consumer. It accepts a list of objects of type T and performs
   *     given operation
   * @param batchSize - the number of objects the BatchingConsumer accepts before passing a list
   *     of these objects to the child consumer
   */
  public BatchingConsumer(Consumer<List<T>> child, int batchSize) {
    this.child = child;
    this.batchSize = batchSize;
    this.buffer = new ArrayList<T>(batchSize);
  }

  /**
   * Performs this operation on the given argument. Consumes batchSize number of input arguments,
   * before calling the child consumer with a list of collected items in the batch.
   *
   * @param t - the input argument
   */
  public void accept(T t) {
    buffer.add(t);
    if (buffer.size() >= batchSize) {
      child.accept(buffer);
      buffer.clear();
    }
  }

  /**
   * Performs the given operation on remaining items in the buffer not processed by previous accept
   * calls.
   */
  public void close() {
    if (!buffer.isEmpty()) {
      child.accept(buffer);
    }
    if (child instanceof AutoCloseable) {
      try {
        ((AutoCloseable) child).close();
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }
}

