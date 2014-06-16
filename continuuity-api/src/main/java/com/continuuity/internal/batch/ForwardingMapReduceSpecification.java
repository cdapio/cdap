/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.batch;

import com.continuuity.api.mapreduce.MapReduceSpecification;

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
