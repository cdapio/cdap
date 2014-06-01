package com.continuuity.api.data;

import com.continuuity.internal.data.dataset.DatasetInstanceProperties;

/**
 * Information for creating dataset instance.
 */
public final class DatasetInstanceCreationSpec {
  private final String instanceName;
  private final String typeName;
  private final DatasetInstanceProperties props;

  public DatasetInstanceCreationSpec(String instanceName, String typeName, DatasetInstanceProperties props) {
    this.instanceName = instanceName;
    this.typeName = typeName;
    this.props = props;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public String getTypeName() {
    return typeName;
  }

  public DatasetInstanceProperties getProperties() {
    return props;
  }
}
