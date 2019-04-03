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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Defines split of the dataset.
 * <b>
 *   Typically a dataset is spit into multiple chunks that are fed into a batch job.
 * </b>
 *
 * Sub-class can implements the {@link #writeExternal(DataOutput)} and the {@link #readExternal(DataInput)}
 * and have a public default constructor (constructor without arguments)
 * to provide a more efficient serialization mechanism.
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

  /**
   * Serializing this split.
   *
   * @param out the {@link DataOutput} for writing out this split information
   * @throws IOException if failed to serialize
   */
  public void writeExternal(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Deserialize this split.
   *
   * @param in the {@link DataInput} to read back the split information.
   * @throws IOException if failed to deserialize
   */
  public void readExternal(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }
}
