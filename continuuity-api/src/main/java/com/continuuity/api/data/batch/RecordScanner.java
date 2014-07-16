/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.api.data.batch;

import com.continuuity.api.annotation.Beta;

/**
 * Defines a reader of a dataset {@link com.continuuity.api.data.batch.Split}.
 * @param <RECORD> the type of objects that represents a single record
 */
@Beta
public abstract class RecordScanner<RECORD> {

  /**
   * Called once at initialization.
   * @param split The split that defines the range of records to read.
   * @throws InterruptedException
   */
  public abstract void initialize(Split split) throws InterruptedException;

  /**
   * Read the next record.
   * @return true if a record was read.
   * @throws InterruptedException
   */
  public abstract boolean nextRecord() throws InterruptedException;

  /**
   * Get the current record.
   * @return The current record, or null if there is no current record.
   * @throws InterruptedException
   */
  public abstract RECORD getCurrentRecord() throws InterruptedException;

  /**
   * The current progress of the record reader through its data.
   * By default progress is not reported in the middle of split reading.
   * @return A number between 0.0 and 1.0 that is the fraction of the data that has been read.
   * @throws InterruptedException
   */
  public float getProgress() throws InterruptedException {
    return 0;
  }

  /**
   * Close the record reader.
   */
  public abstract void close();
}
