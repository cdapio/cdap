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

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;

import java.util.Map;

/**
 * Internal helper class to represent an output format provider and its configuration.
 */
public class ProvidedOutput {
  private final Output output;
  private final OutputFormatProvider outputFormatProvider;
  private final String outputFormatClassName;
  private final Map<String, String> outputFormatConfiguration;

  public ProvidedOutput(Output originalOutput, OutputFormatProvider outputFormatProvider) {
    this.output = originalOutput;
    this.outputFormatProvider = outputFormatProvider;
    this.outputFormatClassName = outputFormatProvider.getOutputFormatClassName();
    this.outputFormatConfiguration = outputFormatProvider.getOutputFormatConfiguration();
    if (outputFormatClassName == null) {
      throw new IllegalArgumentException(String.format("Output '%s' provided null as the output format",
                                                       output.getAlias()));
    }
    if (outputFormatConfiguration == null) {
      throw new IllegalArgumentException(String.format("Output '%s' provided null as the output format configuration",
                                                       output.getAlias()));
    }
  }

  public ProvidedOutput(Output output,
                        OutputFormatProvider outputFormatProvider,
                        String outputFormatClassName,
                        Map<String, String> outputFormatConfiguration) {
    this.output = output;
    this.outputFormatProvider = outputFormatProvider;
    this.outputFormatClassName = outputFormatClassName;
    this.outputFormatConfiguration = outputFormatConfiguration;
  }

  /**
   * Returns the original Output used to create this ProvidedOutput.
   */
  public Output getOutput() {
    return output;
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
