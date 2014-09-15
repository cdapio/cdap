/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.shell;

import co.cask.cdap.client.ApplicationClient;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.shell.completer.element.AppIdCompleter;
import co.cask.cdap.shell.completer.element.DatasetModuleNameCompleter;
import co.cask.cdap.shell.completer.element.DatasetNameCompleter;
import co.cask.cdap.shell.completer.element.DatasetTypeNameCompleter;
import co.cask.cdap.shell.completer.element.ProgramIdCompleter;
import co.cask.cdap.shell.completer.element.StreamIdCompleter;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import jline.console.completer.Completer;
import jline.console.completer.FileNameCompleter;

import java.util.Map;
import javax.inject.Inject;

/**
 * Default set of completers.
 */
public class DefaultCompleters extends CompleterSet {

  @Inject
  public DefaultCompleters(Injector injector) {
    super(
      ImmutableMap.<String, Completer>builder()
        .put(ArgumentName.APP.getName(), injector.getInstance(AppIdCompleter.class))
        .put(ArgumentName.DATASET_MODULE.getName(), injector.getInstance(DatasetModuleNameCompleter.class))
        .put(ArgumentName.DATASET.getName(), injector.getInstance(DatasetNameCompleter.class))
        .put(ArgumentName.DATASET_TYPE.getName(), injector.getInstance(DatasetTypeNameCompleter.class))
        .put(ArgumentName.STREAM.getName(), injector.getInstance(StreamIdCompleter.class))
        .put(ArgumentName.APP_JAR_FILE.getName(), new FileNameCompleter())
        .put(ArgumentName.DATASET_MODULE_JAR_FILE.getName(), new FileNameCompleter())
        .putAll(generateProgramIdCompleters(injector))
        .build());
  }

  private static Map<? extends String, ? extends Completer> generateProgramIdCompleters(Injector injector) {
    ImmutableMap.Builder<String, Completer> result = ImmutableMap.builder();
    for (ElementType elementType : ElementType.values()) {
      if (elementType.getProgramType() != null && elementType.isListable()) {
        result.put(elementType.getArgumentName().getName(),
                   new ProgramIdCompleter(injector.getInstance(ApplicationClient.class), elementType.getProgramType()));
      }
    }
    return result.build();
  }

}
