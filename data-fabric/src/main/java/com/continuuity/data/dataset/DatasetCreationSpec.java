/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.data.dataset;

import com.continuuity.api.dataset.DatasetProperties;

/**
 * Information for creating dataset instance.
 */
public final class DatasetCreationSpec {
  private final String instanceName;
  private final String typeName;
  private final DatasetProperties props;

  public DatasetCreationSpec(String instanceName, String typeName, DatasetProperties props) {
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

  public DatasetProperties getProperties() {
    return props;
  }
}
