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
