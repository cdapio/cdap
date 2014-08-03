/*
 * Copyright 2014 Cask, Inc.
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

/**
 * Defines split of the dataset.
 * <b>
 *   Typically a dataset is spit into multiple chunks that are fed into a batch job.
 * </b>
 */
public abstract class Split {
  /**
   * By default assume that the size of each split is roughly the same.
   *
   * @return Optional split length. Used only for tracking split consuming completion percentile.
   */
  public long getLength() {
    return 0;
  }
}
