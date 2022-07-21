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

package io.cdap.cdap.etl.api.engine.sql.dataset;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.etl.api.ToKeyValueTransform;

/**
 * SQL Dataset which exposes an {@link OutputFormatProvider} and {@link ToKeyValueTransform} used to push records
 * into the SQL engine.
 *
 * @param <T> the type of the records to push to the SQL engine
 * @param <K> Type for output key when mapping records using the {@link ToKeyValueTransform}
 * @param <V> Type for output value when mapping records using the {@link ToKeyValueTransform}
 */
public interface SQLPushDataset<T, K, V> extends SQLDataset, OutputFormatProvider, ToKeyValueTransform<T, K, V> {
}
