package com.continuuity.internal.app.services.legacy;

import java.util.Set;

/**
 * Provides the definition.
 */
public interface QueryDefinition {
  public String getQueryProvider();
  public String getServiceName();
  public QueryProviderContentType getContentType();
  public long getProcessTimeout();
  public Set<String> getDatasets();
}
