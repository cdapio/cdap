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

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Interface for a dataset that a batch job can output to, as series of records (as apposed to key/value pairs).
 * See {@link BatchWritable}.
 * @param <RECORD> the type of objects that represents a single record
 */
@Beta
public interface RecordWritable<RECORD> extends Closeable {

  /**
   * The type of records that the dataset exposes as a schema. The schema will be derived from the type
   * using reflection.
   * @return the schema type.
   */
  Type getRecordType();

  /**
   * Writes the record into a dataset.
   *
   * @param record record to write into the dataset.
   * @throws IOException when the {@code RECORD} could not be written to the dataset.
   */
  public void write(RECORD record) throws IOException;
}
