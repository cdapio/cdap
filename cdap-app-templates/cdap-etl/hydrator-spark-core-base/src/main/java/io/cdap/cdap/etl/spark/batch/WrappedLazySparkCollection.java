/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.batch;


import io.cdap.cdap.etl.spark.SparkCollection;

import java.util.function.Function;

/**
 * FutureCollection implementation that wraps a {@link LazySparkCollection} in order to delay execution of the
 * mapping function.
 * @param <T> Type of the wrapped collection records.
 * @param <U> Type of the output collection records.
 */
public class WrappedLazySparkCollection<T, U>
  extends WrappedCollection<T, U>
  implements SparkCollection<U>, FutureCollection<U>, WrappableCollection<U> {

  FutureCollection<T> wrapped;

  public WrappedLazySparkCollection(FutureCollection<T> wrapped,
                                    java.util.function.Function<SparkCollection<T>, SparkCollection<U>> mapper) {
    super(wrapped, mapper);
    this.wrapped = wrapped;
  }

  @Override
  protected SparkCollection<U> unwrap() {
    // Resolve this collection if needed.
    SparkCollection<T> resolved = wrapped.resolve();

    if (unwrapped == null) {
      unwrapped = mapper.apply(resolved);
    }

    return unwrapped;
  }

  /**
   * Method used to resolve the wrapped collection.
   *
   * If the underlying collection is a {@link SQLBackedCollection}, the collection is wrapped to delay execution
   * until records are needed in Spark.
   * @return resolved {@link SparkCollection} instance
   */
  @Override
  public SparkCollection<U> resolve() {
    SparkCollection<T> resolved = wrapped.resolve();

    // If the resolved instance is a backed collection, wrap it in order to delay execution.
    if (resolved instanceof SQLBackedCollection) {
      return new WrappedSQLEngineCollection<>((SQLBackedCollection<T>) resolved, mapper);
    }

    // Return the transformed collection.
    return mapper.apply(wrapped);
  }

  @Override
  @SuppressWarnings("raw,unchecked")
  public WrappedCollection<U, ?> wrap(java.util.function.Function<SparkCollection<U>, SparkCollection<?>> mapper) {
    return new WrappedLazySparkCollection(this, mapper);
  }
}
