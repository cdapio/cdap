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

package co.cask.cdap.etl.common;

import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Macro evaluator used by batch application
 */
public class DefaultMacroEvaluator implements MacroEvaluator {
  private final WorkflowToken workflowToken;
  private final Map<String, String> runtimeArguments;
  private final long logicalStartTime;

  public DefaultMacroEvaluator(WorkflowToken workflowToken, Map<String, String> runtimeArguments,
                               long logicalStartTime) {
    this.workflowToken = workflowToken;
    this.runtimeArguments = runtimeArguments;
    this.logicalStartTime = logicalStartTime;
  }

  @Override
  @Nullable
  public String lookup(String property) {
    Value tokenValue = workflowToken.get(property);
    if (tokenValue != null) {
      return tokenValue.toString();
    }
    return runtimeArguments.get(property);
  }

  @Override
  @Nullable
  public String evaluate(String macroFunction, String... arguments) {
    // todo: use secure store macro function or logicalStartTime macro function.
    return null;
  }
}
