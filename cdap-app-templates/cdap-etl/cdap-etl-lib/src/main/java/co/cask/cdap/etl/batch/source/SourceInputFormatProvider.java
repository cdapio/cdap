/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.batch.source;

import co.cask.cdap.api.data.batch.InputFormatProvider;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link InputFormatProvider} based on {@link Configuration} and input format class.
 */
public final class SourceInputFormatProvider implements InputFormatProvider {

  private final String inputFormatClassName;
  private final Map<String, String> configuration;

  public SourceInputFormatProvider(Class<? extends InputFormat> inputFormatClass, Configuration hConf) {
    this(inputFormatClass.getName(), hConf);
  }

  public SourceInputFormatProvider(String inputFormatClassName, Configuration hConf) {
    Map<String, String> config = new HashMap<>();
    for (Map.Entry<String, String> entry : hConf) {
      config.put(entry.getKey(), entry.getValue());
    }

    this.inputFormatClassName = inputFormatClassName;
    this.configuration = config;
  }

  public SourceInputFormatProvider(String inputFormatClassName, Map<String, String> configuration) {
    this.inputFormatClassName = inputFormatClassName;
    this.configuration = ImmutableMap.copyOf(configuration);
  }

  @Override
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  @Override
  public Map<String, String> getInputFormatConfiguration() {
    return configuration;
  }
}
