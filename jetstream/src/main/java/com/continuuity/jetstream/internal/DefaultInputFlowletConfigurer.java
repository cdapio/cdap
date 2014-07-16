/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.jetstream.internal;

import com.continuuity.jetstream.api.AbstractInputFlowlet;
import com.continuuity.jetstream.api.InputFlowletConfigurer;
import com.continuuity.jetstream.api.StreamSchema;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Default InputFlowletConfigurer.
 */
public class DefaultInputFlowletConfigurer implements InputFlowletConfigurer {
  private String name;
  private String description;
  private Map<String, StreamSchema> inputGDATStreams = Maps.newHashMap();
  private Map<String, String> sqlMap = Maps.newHashMap();

  public DefaultInputFlowletConfigurer(AbstractInputFlowlet flowlet) {
    this.name = flowlet.getClass().getSimpleName();
    this.description = "";
  }

  @Override
  public void setName(String name) {
    Preconditions.checkArgument(name != null, "Name cannot be null.");
    this.name = name;
  }

  @Override
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public void addGDATInput(String name, StreamSchema schema) {
    Preconditions.checkArgument(name != null, "Input Name cannot be null.");
    Preconditions.checkState(!inputGDATStreams.containsKey(name), "Input Name already exists.");
    this.inputGDATStreams.put(name, schema);
  }

  @Override
  public void addGSQL(String outputName, String gsql) {
    Preconditions.checkArgument(outputName != null, "Output Name cannot be null.");
    Preconditions.checkState(!sqlMap.containsKey(outputName), "Output Name already exists.");
    this.sqlMap.put(outputName, gsql);
  }

  public InputFlowletSpecification createInputFlowletSpec() {
    return new DefaultInputFlowletSpecification(name, description, inputGDATStreams, sqlMap);
  }
}
