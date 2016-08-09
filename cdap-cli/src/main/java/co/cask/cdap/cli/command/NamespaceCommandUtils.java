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

package co.cask.cdap.cli.command;

import co.cask.cdap.proto.NamespaceConfig;

/**
 * Helper class for all utils used in Namespace Commands
 */
public final class NamespaceCommandUtils {

  private NamespaceCommandUtils() {
  }

  /**
   * Pretty print only non-empty field of {@link NamespaceConfig} for CLI usage
   *
   * @return the String to print
   */
  public static String prettyPrintNamespaceConfigCLI(NamespaceConfig namespaceConfig) {
    StringBuilder builder = new StringBuilder();
    if (!namespaceConfig.getSchedulerQueueName().isEmpty()) {
      builder.append("scheduler-queue-name='").append(namespaceConfig.getSchedulerQueueName()).append("', ");
    }
    if (namespaceConfig.getRootDirectory() != null) {
      builder.append("root-directory='").append(namespaceConfig.getRootDirectory()).append("', ");
    }
    if (namespaceConfig.getHbaseNamespace() != null) {
      builder.append("hbase-namespace='").append(namespaceConfig.getHbaseNamespace()).append("', ");
    }
    if (namespaceConfig.getHiveDatabase() != null) {
      builder.append("hive-database='").append(namespaceConfig.getHiveDatabase()).append("', ");
    }
    if (namespaceConfig.getPrincipal() != null) {
      builder.append("principal='").append(namespaceConfig.getPrincipal()).append("', ");
    }
    if (namespaceConfig.getKeytabURI() != null) {
      builder.append("keytab-URI='").append(namespaceConfig.getKeytabURI()).append("', ");
    }
    // Remove the final ", "
    if (builder.length() > 0) {
      builder.delete(builder.length() - 2, builder.length());
    }
    return builder.toString();
  }
}
