/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.dataset.lib.KeyValue;

/**
 * Interface for an entity which allows the transformation between an object of a given Type into a KeyValue pair.
 *
 * This interface is used when mapping records into and out of storage engines, and allows us to reuse some of the
 * logic present in our existing Batch Sinks.
 *
 * @param <TYPE> The Record Type
 * @param <KEY_OUT> Type for output key when mapping records
 * @param <VALUE_OUT> Type for output value when mapping records
 */
@Beta
public interface ToKeyValueTransform<TYPE, KEY_OUT, VALUE_OUT> {
  Transform<TYPE, KeyValue<KEY_OUT, VALUE_OUT>> toKeyValue();
}
