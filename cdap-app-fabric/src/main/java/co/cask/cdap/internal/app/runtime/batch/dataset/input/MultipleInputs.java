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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * This utility class supports MapReduce jobs that have multiple inputs with
 * a different {@link InputFormat} and {@link Mapper} for each path
 */
public final class MultipleInputs {
  private static final String INPUT_CONFIGS = "mapreduce.input.multipleinputs.inputs";

  private static final Gson GSON = new GsonBuilder().enableComplexMapKeySerialization().create();
  private static final Type STRING_MAPPER_INPUT_MAP_TYPE = new TypeToken<Map<String, MapperInput>>() { }.getType();

  private MultipleInputs() {
  }

  /**
   * Add a {@link Path} with a custom {@link InputFormat} and
   * {@link Mapper} to the list of inputs for the map-reduce job.
   *
   * @param job The {@link Job}
   * @param namedInput name of the input
   * @param inputFormatClass the name of the InputFormat class to be used for this input
   * @param inputConfigs the configurations to be used for this input
   * @param mapperClass {@link Mapper} class to use for this path
   */
  @SuppressWarnings("unchecked")
  public static void addInput(Job job, String namedInput, String inputFormatClass, Map<String, String> inputConfigs,
                              Class<? extends Mapper> mapperClass) {
    Configuration conf = job.getConfiguration();

    Map<String, MapperInput> map = getInputMap(conf);
    if (map.containsKey(namedInput)) {
      throw new IllegalArgumentException("Input already exists with name: " + namedInput);
    }
    map.put(namedInput, new MapperInput(inputFormatClass, inputConfigs, mapperClass));
    conf.set(INPUT_CONFIGS, GSON.toJson(map));

    job.setInputFormatClass(DelegatingInputFormat.class);
  }

  /**
   * @param conf the Configuration from which to deserialize the input configurations
   * @return a mapping from input name to the MapperInput for that input
   */
  public static Map<String, MapperInput> getInputMap(Configuration conf) {
    String mapString = conf.get(INPUT_CONFIGS);
    if (mapString == null) {
      return new HashMap<>();
    }
    return GSON.fromJson(mapString, STRING_MAPPER_INPUT_MAP_TYPE);
  }

  /**
   * A simple POJO for encapsulating information for an input. We don't use
   * {@link co.cask.cdap.internal.app.runtime.batch.dataset.input.MapperInput}, because that consists of an Interface,
   * which doesn't serialize well.
   */
  public static final class MapperInput {
    private final String inputFormatClassName;
    private final Map<String, String> inputFormatConfiguration;
    private final String mapperClassName;

    public MapperInput(String inputFormatClassName, Map<String, String> inputFormatConfiguration,
                       Class<? extends Mapper> mapper) {
      this.inputFormatClassName = inputFormatClassName;
      this.inputFormatConfiguration = inputFormatConfiguration;
      this.mapperClassName = mapper.getName();
    }

    public String getInputFormatClassName() {
      return inputFormatClassName;
    }

    public Map<String, String> getInputFormatConfiguration() {
      return inputFormatConfiguration;
    }

    public String getMapperClassName() {
      return mapperClassName;
    }
  }
}
