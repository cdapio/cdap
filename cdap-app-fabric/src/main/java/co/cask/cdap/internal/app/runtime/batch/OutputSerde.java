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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.batch.dataset.DatasetOutputFormatProvider;
import co.cask.cdap.internal.app.runtime.batch.dataset.output.ProvidedOutput;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by alianwar on 8/16/17.
 */
public class OutputSerde {
  private static final Gson GSON = new GsonBuilder().registerTypeAdapter(Output.class, new OutputCodec()).create();
  private static final Type OUTPUT_LIST_TYPE = new TypeToken<List<Output>>() { }.getType();

  public static void setOutputs(Configuration conf, Collection<Output> outputs) {
    conf.set("cdap.outputs", GSON.toJson(outputs, OUTPUT_LIST_TYPE));
  }

  public static List<Output> getOutputs(Configuration conf) {
    String s = conf.get("cdap.outputs");
    return GSON.fromJson(s, OUTPUT_LIST_TYPE);
  }

  public static Map<String, ProvidedOutput> transform(Collection<Output> outputs, AbstractContext abstractContext) {
    Map<String, ProvidedOutput> outputMap = new LinkedHashMap<>(outputs.size());
    for (Output output : outputs) {
      // TODO: remove alias from ProvidedOutput class?
      outputMap.put(output.getAlias(), new ProvidedOutput(output.getAlias(), transform(output, abstractContext)));
    }
    return outputMap;
  }


  private static OutputFormatProvider transform(Output output, AbstractContext abstractContext) {
    if (output instanceof Output.DatasetOutput) {
      Output.DatasetOutput datasetOutput = (Output.DatasetOutput) output;
      String datasetNamespace = datasetOutput.getNamespace();
      if (datasetNamespace == null) {
        datasetNamespace = abstractContext.getNamespace();
      }
      String datasetName = output.getName();
      Map<String, String> args = datasetOutput.getArguments();
      Dataset dataset = abstractContext.getDataset(datasetNamespace, datasetName, args, AccessType.WRITE);
      return new DatasetOutputFormatProvider(datasetNamespace, datasetName, args, dataset);

    } else if (output instanceof Output.OutputFormatProviderOutput) {
      return ((Output.OutputFormatProviderOutput) output).getOutputFormatProvider();
    } else {
      // shouldn't happen unless user defines their own Output class
      throw new IllegalArgumentException(String.format("Output %s has unknown output class %s",
                                                       output.getName(), output.getClass().getCanonicalName()));
    }
  }
}
