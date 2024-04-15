/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.namespace;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages namespaces on underlying systems - HDFS, HBase, Hive, etc.
 */
public final class DistributedStorageProviderNamespaceAdmin extends
    AbstractStorageProviderNamespaceAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(
      DistributedStorageProviderNamespaceAdmin.class);

  private final Configuration hConf;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  DistributedStorageProviderNamespaceAdmin(CConfiguration cConf,
      NamespacePathLocator namespacePathLocator,
      NamespaceQueryAdmin namespaceQueryAdmin) {
    super(cConf, namespacePathLocator, namespaceQueryAdmin);
    this.hConf = new Configuration();
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public void create(NamespaceMeta namespaceMeta) throws IOException, SQLException {
    // create filesystem directory
    super.create(namespaceMeta);
  }

  @SuppressWarnings("ConstantConditions")
  @Override
  public void delete(NamespaceId namespaceId) throws IOException, SQLException {
    // delete namespace directory from filesystem
    super.delete(namespaceId);
    if (NamespaceId.DEFAULT.equals(namespaceId)) {
      return;
    }
  }
}
