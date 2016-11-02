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

import co.cask.cdap.api.data.batch.InputFormatProvider;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Encapsulates {@link InputFormatProvider} and a {@link Mapper} to use for that input.
 */
public class MapperInput {
  private final String alias;
  private final InputFormatProvider inputFormatProvider;
  private final String inputFormatClassName;
  private final Map<String, String> inputFormatConfiguration;
  private final Class<? extends Mapper> mapper;

  /**
   * Creates an instance of MapperInput with the given InputFormatProvider and specified Mapper class.
   */
  public MapperInput(String alias, InputFormatProvider inputFormatProvider, @Nullable Class<? extends Mapper> mapper) {
    this.alias = alias;
    this.inputFormatProvider = inputFormatProvider;
    this.inputFormatClassName = inputFormatProvider.getInputFormatClassName();
    this.inputFormatConfiguration = inputFormatProvider.getInputFormatConfiguration();
    this.mapper = mapper;
    if (inputFormatClassName == null) {
      throw new IllegalArgumentException(
        "Input '" + alias + "' provided null as the input format");
    }
    if (inputFormatConfiguration == null) {
      throw new IllegalArgumentException(
        "Input '" + alias + "' provided null as the input format configuration");
    }
  }

  public String getAlias() {
    return alias;
  }

  public InputFormatProvider getInputFormatProvider() {
    return inputFormatProvider;
  }

  public String getInputFormatClassName() {
    return inputFormatClassName;
  }

  public Map<String, String> getInputFormatConfiguration() {
    return inputFormatConfiguration;
  }

  @Nullable
  public Class<? extends Mapper> getMapper() {
    return mapper;
  }
}
