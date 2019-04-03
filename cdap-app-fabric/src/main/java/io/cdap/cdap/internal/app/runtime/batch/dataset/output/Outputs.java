/*
 * Copyright © 2017 Cask Data, Inc.
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
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.batch.dataset.DatasetOutputFormatProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility class to help deal with Outputs.
 */
public final class Outputs {
  private Outputs() { }

  /**
   * Transforms a list of {@link Output}s to {@link ProvidedOutput}.
   */
  public static List<ProvidedOutput> transform(List<Output.DatasetOutput> outputs,
                                               final AbstractContext abstractContext) {
    // we don't want to use Lists.transform, to catch any errors with transform earlier on
    List<ProvidedOutput> providedOutputs = new ArrayList<>(outputs.size());
    for (Output.DatasetOutput output : outputs) {
      providedOutputs.add(transform(output, abstractContext));
    }
    return providedOutputs;
  }

  public static ProvidedOutput transform(Output.DatasetOutput datasetOutput,
                                         AbstractContext abstractContext) {
    String datasetNamespace = datasetOutput.getNamespace();
    if (datasetNamespace == null) {
      datasetNamespace = abstractContext.getNamespace();
    }
    String datasetName = datasetOutput.getName();
    Map<String, String> args = datasetOutput.getArguments();
    Dataset dataset = abstractContext.getDataset(datasetNamespace, datasetName, args, AccessType.WRITE);
    DatasetOutputFormatProvider datasetOutputFormatProvider =
      new DatasetOutputFormatProvider(datasetNamespace, datasetName, args, dataset);
    return new ProvidedOutput(datasetOutput, datasetOutputFormatProvider);
  }
}
