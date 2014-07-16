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

import java.io.Closeable;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Represents data sets that can be processed in batches, as series of records (as apposed to key/value pairs). See
 * {@link BatchReadable}.
 * @param <RECORD> the type of objects that represents a single record
 */
@Beta
public interface RecordScannable<RECORD> extends Closeable {

  /**
   * This method is needed because Java does not remember the RECORD type parameter at runtime.
   * @return the schema type, that is RECORD.
   */
  Type getRecordType();

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
   * @return The instance of a {@link RecordScanner}.
   */
  RecordScanner<RECORD> createSplitRecordScanner(Split split);
}
