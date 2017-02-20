/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

import co.cask.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

/**
 * Internal helper class to represent an output format provider and its configuration.
 */
public class ProvidedOutput {
  private final String alias;
  private final OutputFormatProvider outputFormatProvider;
  private final String outputFormatClassName;
  private final Map<String, String> outputFormatConfiguration;

  public ProvidedOutput(String alias, OutputFormatProvider outputFormatProvider) {
    this.alias = alias;
    this.outputFormatProvider = outputFormatProvider;
    this.outputFormatClassName = outputFormatProvider.getOutputFormatClassName();
    this.outputFormatConfiguration = outputFormatProvider.getOutputFormatConfiguration();
    if (outputFormatClassName == null) {
      throw new IllegalArgumentException("Output '" + alias + "' provided null as the output format");
    }
    if (outputFormatConfiguration == null) {
      throw new IllegalArgumentException("Output '" + alias + "' provided null as the output format configuration");
    }
  }

  public ProvidedOutput(String alias,
                        OutputFormatProvider outputFormatProvider,
                        String outputFormatClassName,
                        Map<String, String> outputFormatConfiguration) {
    this.alias = alias;
    this.outputFormatProvider = outputFormatProvider;
    this.outputFormatClassName = outputFormatClassName;
    this.outputFormatConfiguration = outputFormatConfiguration;
  }

  public String getAlias() {
    return alias;
  }

  public OutputFormatProvider getOutputFormatProvider() {
    return outputFormatProvider;
  }

  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  public Map<String, String> getOutputFormatConfiguration() {
    return outputFormatConfiguration;
  }
}
