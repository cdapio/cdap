/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.cli.command.system.RenderAsCommand;
import io.cdap.cdap.cli.completer.element.AppIdCompleter;
import io.cdap.cdap.cli.completer.element.ArtifactNameCompleter;
import io.cdap.cdap.cli.completer.element.DatasetModuleNameCompleter;
import io.cdap.cdap.cli.completer.element.DatasetNameCompleter;
import io.cdap.cdap.cli.completer.element.DatasetTypeNameCompleter;
import io.cdap.cdap.cli.completer.element.EndpointCompleter;
import io.cdap.cdap.cli.completer.element.NamespaceNameCompleter;
import io.cdap.cdap.cli.completer.element.ProgramIdCompleter;
import io.cdap.cdap.client.ApplicationClient;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.security.Principal;
import jline.console.completer.Completer;
import jline.console.completer.EnumCompleter;
import jline.console.completer.FileNameCompleter;

import java.util.Map;
import javax.inject.Inject;

/**
 * Default set of completers.
 */
public class DefaultCompleters implements Supplier<Map<String, Completer>> {

  private final ImmutableMap<String, Completer> completers;

  @Inject
  public DefaultCompleters(Injector injector) {
    this.completers = ImmutableMap.<String, Completer>builder()
        .put(ArgumentName.APP.getName(), injector.getInstance(AppIdCompleter.class))
        .put(ArgumentName.ARTIFACT_NAME.getName(), injector.getInstance(ArtifactNameCompleter.class))
        .put(ArgumentName.DATASET_MODULE.getName(), injector.getInstance(DatasetModuleNameCompleter.class))
        .put(ArgumentName.DATASET.getName(), injector.getInstance(DatasetNameCompleter.class))
        .put(ArgumentName.DATASET_TYPE.getName(), injector.getInstance(DatasetTypeNameCompleter.class))
        .put(ArgumentName.LOCAL_FILE_PATH.getName(), new FileNameCompleter())
        .put(ArgumentName.APP_JAR_FILE.getName(), new FileNameCompleter())
        .put(ArgumentName.DATASET_MODULE_JAR_FILE.getName(), new FileNameCompleter())
        .put(ArgumentName.ARTIFACT_CONFIG_FILE.getName(), new FileNameCompleter())
        .put(ArgumentName.APP_CONFIG_FILE.getName(), new FileNameCompleter())
        .put(ArgumentName.HTTP_METHOD.getName(), new EndpointCompleter())
        .put(ArgumentName.ENDPOINT.getName(), new EndpointCompleter())
        .put(ArgumentName.RUN_STATUS.getName(), new EnumCompleter(ProgramRunStatus.class))
        .put(ArgumentName.NAMESPACE_NAME.getName(), injector.getInstance(NamespaceNameCompleter.class))
        .put(ArgumentName.COMMAND_CATEGORY.getName(), new EnumCompleter(CommandCategory.class))
        .put(ArgumentName.TABLE_RENDERER.getName(), new EnumCompleter(RenderAsCommand.Type.class))
        .put(ArgumentName.WORKFLOW_TOKEN_SCOPE.getName(), new EnumCompleter(WorkflowToken.Scope.class))
        .put(ArgumentName.METADATA_SCOPE.getName(), new EnumCompleter(MetadataScope.class))
        .put(ArgumentName.PRINCIPAL_TYPE.getName(), new EnumCompleter(Principal.PrincipalType.class))
        .putAll(generateProgramIdCompleters(injector)).build();
  }

  private static Map<? extends String, ? extends Completer> generateProgramIdCompleters(Injector injector) {
    ImmutableMap.Builder<String, Completer> result = ImmutableMap.builder();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.getProgramType() != null && elementType.isListable()) {
        result.put(elementType.getArgumentName().getName(),
                   new ProgramIdCompleter(injector.getInstance(ApplicationClient.class),
                                          injector.getInstance(CLIConfig.class),
                                          elementType.getProgramType()));
      }
    }
    return result.build();
  }

  @Override
  public Map<String, Completer> get() {
    return completers;
  }
}
