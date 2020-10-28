/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch.preview;

import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.etl.batch.BasicInputFormatProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * An InputFormatProvider that limits how much data is read.
 */
public class LimitingInputFormatProvider extends BasicInputFormatProvider {

  public LimitingInputFormatProvider(InputFormatProvider delegate, int maxRecords) {
    super(LimitingInputFormat.class.getName(), getConfiguration(delegate, maxRecords));
  }

  private static Map<String, String> getConfiguration(InputFormatProvider delegate, int maxRecords) {
    Map<String, String> config = new HashMap<>(delegate.getInputFormatConfiguration());
    config.put(LimitingInputFormat.DELEGATE_CLASS_NAME, delegate.getInputFormatClassName());
    config.put(LimitingInputFormat.MAX_RECORDS, String.valueOf(maxRecords));
    return config;
  }
}
