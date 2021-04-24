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
 *
 */

package io.cdap.cdap.etl.api.batch;


import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.SampleRequest;

import java.util.List;

/**
 * Batch connector that relies on the {@link InputFormatProvider} to read from the resources
 *
 * @param <VAL_IN>
 */
public interface BatchConnector<VAL_IN> extends Connector {

  /**
   * Return the input format this connector will use to do the sampling
   */
  InputFormatProvider getInputFormatProvider(SampleRequest request);

  /**
   * Transform the sampled records back to StructuredRecord
   */
  List<StructuredRecord> transform(List<VAL_IN> records);
}
