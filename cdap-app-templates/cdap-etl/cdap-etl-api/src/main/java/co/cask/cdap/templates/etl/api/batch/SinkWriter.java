/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.templates.etl.api.batch;

/**
 * Writer for used by {@link BatchSink} to write key value pairs for a Batch job.
 *
 * @param <KEY> the type of key to write
 * @param <VAL> the type of value to write
 */
public interface SinkWriter<KEY, VAL> {

  /**
   * Write a key value pair
   *
   * @param key the key to write
   * @param val the value to write
   */
  void write(KEY key, VAL val) throws Exception;
}
