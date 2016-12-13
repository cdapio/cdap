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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

/**
 * A {@link TaggedInputSplit} that is tagged with extra data for use by {@link MultiInputFormat}s.
 */
public class MultiInputTaggedSplit extends TaggedInputSplit {

  private static final Type STRING_STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();

  private String name;
  private Map<String, String> inputConfigs;
  private String mapperClassName;

  private Class<? extends InputFormat<?, ?>> inputFormatClass;

  MultiInputTaggedSplit() { }

  /**
   * Creates a new MultiInputTaggedSplit.
   *
   * @param inputSplit The InputSplit to be tagged
   * @param conf The configuration to use
   * @param name name of the input to which this InputSplit belongs
   * @param inputConfigs configurations to use for the InputFormat
   * @param inputFormatClass The InputFormat class to use for this job
   * @param mapperClassName The name of the Mapper class to use for this job
   */
  @SuppressWarnings("unchecked")
  MultiInputTaggedSplit(InputSplit inputSplit, Configuration conf,
                        String name,
                        Map<String, String> inputConfigs,
                        Class<? extends InputFormat> inputFormatClass,
                        String mapperClassName) {
    super(inputSplit, conf);
    this.name = name;
    this.inputConfigs = inputConfigs;
    this.mapperClassName = mapperClassName;
    this.inputFormatClass = (Class<? extends InputFormat<?, ?>>) inputFormatClass;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void readAdditionalFields(DataInput in) throws IOException {
    name = Text.readString(in);
    mapperClassName = Text.readString(in);
    inputConfigs = GSON.fromJson(Text.readString(in), STRING_STRING_MAP_TYPE);
    inputFormatClass = (Class<? extends InputFormat<?, ?>>) readClass(in);
  }

  @Override
  protected void writeAdditionalFields(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, mapperClassName);
    Text.writeString(out, GSON.toJson(inputConfigs));
    Text.writeString(out, inputFormatClass.getName());
  }

  /**
   * Retrieves the name of this MultiInputTaggedSplit.
   *
   * @return name of the MultiInputTaggedSplit
   */
  public String getName() {
    return name;
  }

  /**
   * Retrieves the name of the Mapper class to use for this split.
   *
   * @return The name of the Mapper class to use
   */
  public String getMapperClassName() {
    return mapperClassName;
  }

  Map<String, String> getInputConfigs() {
    return inputConfigs;
  }

  /**
   * Retrieves the InputFormat class to use for this split.
   *
   * @return The InputFormat class to use
   */
  public Class<? extends InputFormat<?, ?>> getInputFormatClass() {
    return inputFormatClass;
  }
}
