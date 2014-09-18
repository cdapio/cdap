/*
 * Copyright © 2014 Cask Data, Inc.
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
package co.cask.cdap.internal.batch;

import co.cask.cdap.api.mapreduce.MapReduceSpecification;

import java.util.Map;
import java.util.Set;

/**
 *
 */
public abstract class ForwardingMapReduceSpecification implements MapReduceSpecification {

  protected final MapReduceSpecification delegate;

  protected ForwardingMapReduceSpecification(MapReduceSpecification delegate) {
    this.delegate = delegate;
  }

  @Override
  public Set<String> getDataSets() {
    return delegate.getDataSets();
  }

  @Override
  public Map<String, String> getProperties() {
    return delegate.getProperties();
  }

  @Override
  public String getProperty(String key) {
    return delegate.getProperty(key);
  }

  @Override
  public String getOutputDataSet() {
    return delegate.getOutputDataSet();
  }

  @Override
  public String getInputDataSet() {
    return delegate.getInputDataSet();
  }

  @Override
  public String getClassName() {
    return delegate.getClassName();
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public String getDescription() {
    return delegate.getDescription();
  }

  @Override
  public int getMapperMemoryMB() {
    return delegate.getMapperMemoryMB();
  }

  @Override
  public int getReducerMemoryMB() {
    return delegate.getReducerMemoryMB();
  }
}
