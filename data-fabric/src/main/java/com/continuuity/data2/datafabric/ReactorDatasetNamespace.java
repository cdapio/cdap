package com.continuuity.data2.datafabric;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.DataSetAccessor;
import com.continuuity.data2.dataset2.DatasetNamespace;

/**
 * Reactor's dataset namespace.
 */
public class ReactorDatasetNamespace implements DatasetNamespace {
  private final String namespacePrefix;
  private final DataSetAccessor.Namespace namespace;

  public ReactorDatasetNamespace(CConfiguration conf, DataSetAccessor.Namespace namespace) {
    String reactorNameSpace = conf.get(DataSetAccessor.CFG_TABLE_PREFIX, DataSetAccessor.DEFAULT_TABLE_PREFIX);
    this.namespacePrefix = reactorNameSpace + ".";
    this.namespace = namespace;
  }

  @Override
  public String namespace(String name) {
    return namespacePrefix +  namespace.namespace(name);
  }

  @Override
  public String fromNamespaced(String name) {
    return namespace.fromNamespaced(name.substring(namespacePrefix.length()));
  }
}
