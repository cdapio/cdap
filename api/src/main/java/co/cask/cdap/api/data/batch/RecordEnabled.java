/*
 * Copyright 2014 Cask Data, Inc.
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

import java.io.Closeable;
import java.lang.reflect.Type;

/**
 *
 */
public interface RecordEnabled extends Closeable {
  /**
   * This method is needed because Java does not remember the RECORD type parameter at runtime.
   * @return the schema type, that is RECORD.
   */
  Type getRecordType();
}
