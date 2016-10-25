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

package co.cask.cdap.logging.save;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.logging.write.FileMetaDataManager;
import com.google.inject.Inject;

/**
 * Factory to create {@link KafkaLogWriterPlugin}.
 */
public class KafkaLogWriterPluginFactory implements KafkaLogProcessorFactory {
  private final CConfiguration cConfig;
  private final FileMetaDataManager fileMetaDataManager;
  private final RootLocationFactory rootLocationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final CheckpointManagerFactory checkpointManagerFactory;
  private final Impersonator impersonator;
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  @Inject
  public KafkaLogWriterPluginFactory(CConfiguration cConfig, FileMetaDataManager fileMetaDataManager,
                                     RootLocationFactory rootLocationFactory,
                                     NamespaceQueryAdmin namespaceQueryAdmin,
                                     NamespacedLocationFactory namespacedLocationFactory,
                                     CheckpointManagerFactory checkpointManagerFactory, Impersonator impersonator) {
    this.cConfig = cConfig;
    this.fileMetaDataManager = fileMetaDataManager;
    this.rootLocationFactory = rootLocationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.checkpointManagerFactory = checkpointManagerFactory;
    this.impersonator = impersonator;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  @Override
  public KafkaLogProcessor create() throws Exception {
    return new KafkaLogWriterPlugin(cConfig, fileMetaDataManager, checkpointManagerFactory, rootLocationFactory,
                                    namespaceQueryAdmin, namespacedLocationFactory, impersonator);
  }
}
