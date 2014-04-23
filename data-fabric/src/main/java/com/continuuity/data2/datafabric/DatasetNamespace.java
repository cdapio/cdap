package com.continuuity.data2.datafabric;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;

/**
 * todo: This is for backwards-compatibility: migrate namespacing logic when after removing DataSetAccessor
 */
public class DatasetNamespace {
  private final String namespacePrefix;
  com.continuuity.data.DataSetAccessor.Namespace namespace;

  public DatasetNamespace(CConfiguration conf, DataSetAccessor.Namespace namespace) {
    String reactorNameSpace = conf.get(DataSetAccessor.CFG_TABLE_PREFIX, DataSetAccessor.DEFAULT_TABLE_PREFIX);
    this.namespacePrefix = reactorNameSpace + ".";
    this.namespace = namespace;
  }

  public String namespace(String name) {
    return namespacePrefix +  namespace.namespace(name);
  }
}
