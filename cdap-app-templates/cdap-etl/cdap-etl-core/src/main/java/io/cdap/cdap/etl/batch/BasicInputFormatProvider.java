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

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.data.batch.InputFormatProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link InputFormatProvider} that simply take a class name and a set of configurations.
 */
public class BasicInputFormatProvider implements InputFormatProvider {

  private final String inputFormatClassName;
  private final Map<String, String> configuration;

  public BasicInputFormatProvider(String inputFormatClassName, Map<String, String> configuration) {
    this.inputFormatClassName = inputFormatClassName;
    this.configuration = ImmutableMap.copyOf(configuration);
  }

  public BasicInputFormatProvider() {
    this.inputFormatClassName = "";
    this.configuration = new HashMap<>();
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
