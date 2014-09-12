/*
 * Copyright 2014 Cask Data, Inc.
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
import co.cask.cdap.shell.completer.StringsCompleter;
import co.cask.cdap.shell.completer.element.ProgramIdCompleter;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jline.console.completer.Completer;

import java.util.Map;
import javax.inject.Inject;

/**
 * Provides {@link ProgramIdCompleter} implementations per {@link ElementType}.
 */
public class ProgramIdCompleterFactory {

  private final Map<ElementType, ProgramIdCompleter> programIdCompleters;

  @Inject
  public ProgramIdCompleterFactory(ApplicationClient appClient) {
    this.programIdCompleters = ImmutableMap.<ElementType, ProgramIdCompleter>builder()
      .put(ElementType.FLOW,
           new ProgramIdCompleter(appClient, ElementType.FLOW.getProgramType()))
      .put(ElementType.MAPREDUCE,
           new ProgramIdCompleter(appClient, ElementType.MAPREDUCE.getProgramType()))
      .put(ElementType.PROCEDURE,
           new ProgramIdCompleter(appClient, ElementType.PROCEDURE.getProgramType()))
      .put(ElementType.WORKFLOW,
           new ProgramIdCompleter(appClient, ElementType.WORKFLOW.getProgramType()))
      .build();
  }

  public Completer getProgramIdCompleter(ElementType elementType) {
    return Objects.firstNonNull(programIdCompleters.get(elementType),
                                new StringsCompleter(ImmutableList.<String>of()));
  }
}
