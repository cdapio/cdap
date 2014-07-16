/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.internal.app.services.legacy;

import java.util.Set;

/**
 * Implementation of {@code QueryDefinition}
 */
public class QueryDefinitionImpl implements QueryDefinitionModifier, QueryDefinition {
  private String name;
  private QueryProviderContentType type;
  private String provider;
  private long timeout;
  private int instance = 1;

  /**
   * Defines a collection of flowlet definitions.
   */
  private Set<String> datasets;

  @Override
  public Set<String> getDatasets() {
    return datasets;
  }

  public void setDatasets(Set<String> sets) {
    datasets = sets;
  }

  @Override
  public String getQueryProvider() {
    return provider;
  }

  @Override
  public String getServiceName() {
    return name;
  }

  @Override
  public QueryProviderContentType getContentType() {
    return type;
  }

  @Override
  public long getProcessTimeout() {
    return timeout;
  }

  public void setQueryProvider(String provider) {
    this.provider = provider;
  }

  public void setServiceName(String name) {
    this.name = name;
  }

  public void setContentType(QueryProviderContentType type) {
    this.type = type;
  }

  public void setProcessTimeout(long timeout) {
    this.timeout = timeout;
  }

  @Override
  public void setInstances(int newInstances) {
    this.instance = newInstances;
  }

  public int getInstances() {
    return this.instance;
  }
}
