/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.batch;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * An {@link OutputFormatProvider} that simply take a class name and a set of configurations.
 */
public class BasicOutputFormatProvider implements OutputFormatProvider {

  private final String outputFormatClassName;
  private final Map<String, String> outputFormatConfiguration;

  public BasicOutputFormatProvider(String outputFormatClassName, Map<String, String> outputFormatConfiguration) {
    this.outputFormatClassName = outputFormatClassName;
    this.outputFormatConfiguration = Collections.unmodifiableMap(new HashMap<>(outputFormatConfiguration));
  }

  @Override
  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return outputFormatConfiguration;
  }
}
