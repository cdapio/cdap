/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.api.data.batch;

import co.cask.cdap.api.annotation.Beta;

import java.util.List;

/**
 * Interface for datasets that can be input to a batch job.
 * <p>
 *   In order to feed a dataset into a batch job, the dataset must be splittable into chunks so that it's possible 
 *   to process every part of the dataset in parallel. Every chunk must be readable as a collection of {key,value}
 *   records.
 * </p>
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
@Beta
public interface BatchReadable<KEY, VALUE> {
  /**
   * Returns all splits of the dataset.
   * <p>
   *   For feeding the whole dataset into a batch job.
   * </p>
   * @return A list of {@link Split}s.
   */
  List<Split> getSplits();

  /**
   * Creates a reader for the split of a dataset.
   * @param split The split to create a reader for.
   * @return The instance of a {@link SplitReader}.
   */
  SplitReader<KEY, VALUE> createSplitReader(Split split);
}
