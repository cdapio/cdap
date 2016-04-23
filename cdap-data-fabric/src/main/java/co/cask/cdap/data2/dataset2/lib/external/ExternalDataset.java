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

package co.cask.cdap.data2.dataset2.lib.external;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.batch.InputFormatProvider;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Represents a data source/sink that is external to CDAP. No physical manifestation of this dataset exists in CDAP.
 */
@Beta
public class ExternalDataset implements Dataset, InputFormatProvider, OutputFormatProvider {
  private final String inputFormatClassName;
  private final String outputFormatClassName;
  private final Map<String, String> inputFormatConfiguration;
  private final Map<String, String> outputFormatConfiguration;

  public ExternalDataset(Map<String, String> runtimeArgs) {
    // A runtime instantiation of external dataset can be a source or a sink, not both
    this.inputFormatClassName = runtimeArgs.get("input.format.class");
    this.outputFormatClassName = runtimeArgs.get("output.format.class");
    this.inputFormatConfiguration =
      this.inputFormatClassName != null ? runtimeArgs : Collections.<String, String>emptyMap();
    this.outputFormatConfiguration =
      this.outputFormatClassName != null ? runtimeArgs : Collections.<String, String>emptyMap();
  }

  @Override
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return inputFormatConfiguration;
  }

  @Override
  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return outputFormatConfiguration;
  }

  @Override
  public void close() throws IOException {
    // Nothing to do
  }
}
