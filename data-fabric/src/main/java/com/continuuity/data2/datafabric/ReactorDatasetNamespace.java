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
