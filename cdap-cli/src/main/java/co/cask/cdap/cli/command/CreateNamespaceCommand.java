/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.util.AbstractCommand;
import co.cask.cdap.client.NamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.cli.Arguments;
import co.cask.common.cli.Command;
import com.google.inject.Inject;

import java.io.PrintStream;

/**
 * {@link Command} to create a namespace.
 */
public class CreateNamespaceCommand extends AbstractCommand {
  private static final String SUCCESS_MSG = "Namespace '%s' created successfully.";

  private final NamespaceClient namespaceClient;

  @Inject
  public CreateNamespaceCommand(CLIConfig cliConfig, NamespaceClient namespaceClient) {
    super(cliConfig);
    this.namespaceClient = namespaceClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String name = arguments.get(ArgumentName.NAMESPACE_NAME.toString());

    String description = arguments.getOptional(ArgumentName.NAMESPACE_DESCRIPTION.toString(), null);
    String principal = arguments.getOptional(ArgumentName.NAMESPACE_PRINCIPAL.toString(), null);
    String groupName = arguments.getOptional(ArgumentName.NAMESPACE_GROUP_NAME.toString(), null);
    String keytabPath = arguments.getOptional(ArgumentName.NAMESPACE_KEYTAB_PATH.toString(), null);
    String hbaseNamespace = arguments.getOptional(ArgumentName.NAMESPACE_HBASE_NAMESPACE.toString(), null);
    String hiveDatabase = arguments.getOptional(ArgumentName.NAMESPACE_HIVE_DATABASE.toString(), null);
    String schedulerQueueName = arguments.getOptional(ArgumentName.NAMESPACE_SCHEDULER_QUEUENAME.toString(), null);
    String rootDir = arguments.getOptional(ArgumentName.NAMESPACE_ROOT_DIR.toString(), null);

    NamespaceMeta.Builder builder = new NamespaceMeta.Builder();
    builder.setName(name).setDescription(description).setPrincipal(principal).setGroupName(groupName)
      .setKeytabURI(keytabPath).setRootDirectory(rootDir).setHBaseNamespace(hbaseNamespace)
      .setHiveDatabase(hiveDatabase).setSchedulerQueueName(schedulerQueueName);
    namespaceClient.create(builder.build());
    output.println(String.format(SUCCESS_MSG, name));
  }

  @Override
  public String getPattern() {
    return String.format("create namespace <%s> [%s <%s>] [%s <%s>] [%s <%s>] " +
                           "[%s <%s>] [%s <%s>] [%s <%s>] [%s <%s>] [%s <%s>]", ArgumentName.NAMESPACE_NAME,
                         ArgumentName.NAMESPACE_DESCRIPTION, ArgumentName.NAMESPACE_DESCRIPTION,
                         ArgumentName.NAMESPACE_PRINCIPAL, ArgumentName.NAMESPACE_PRINCIPAL,
                         ArgumentName.NAMESPACE_GROUP_NAME, ArgumentName.NAMESPACE_GROUP_NAME,
                         ArgumentName.NAMESPACE_KEYTAB_PATH, ArgumentName.NAMESPACE_KEYTAB_PATH,
                         ArgumentName.NAMESPACE_HBASE_NAMESPACE, ArgumentName.NAMESPACE_HBASE_NAMESPACE,
                         ArgumentName.NAMESPACE_HIVE_DATABASE, ArgumentName.NAMESPACE_HIVE_DATABASE,
                         ArgumentName.NAMESPACE_ROOT_DIR, ArgumentName.NAMESPACE_ROOT_DIR,
                         ArgumentName.NAMESPACE_SCHEDULER_QUEUENAME, ArgumentName.NAMESPACE_SCHEDULER_QUEUENAME);
  }

  @Override
  public String getDescription() {
    return String.format("Creates a %s in CDAP.", ElementType.NAMESPACE.getName());
  }
}
