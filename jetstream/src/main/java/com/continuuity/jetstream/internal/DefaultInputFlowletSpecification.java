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

import com.continuuity.jetstream.api.StreamSchema;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;

import java.util.Map;

/**
 * Default InputFlowlet Specification.
 */
public class DefaultInputFlowletSpecification implements InputFlowletSpecification {
  private String name;
  private String description;
  private Map<String, StreamSchema> gdatInputSchema;
  private Map<String, String> gsql;

  public DefaultInputFlowletSpecification(String name, String description, Map<String, StreamSchema> gdatInputSchema,
                                          Map<String, String> gsql) {
    this.name = name;
    this.description = description;
    this.gdatInputSchema = gdatInputSchema;
    this.gsql = gsql;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public Map<String, StreamSchema> getGdatInputSchema() {
    return gdatInputSchema;
  }

  @Override
  public Map<String, String> getGSQL() {
    return gsql;
  }
}
