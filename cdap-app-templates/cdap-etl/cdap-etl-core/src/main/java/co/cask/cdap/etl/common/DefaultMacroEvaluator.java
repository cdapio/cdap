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

import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.common.macro.LogicalStartTimeMacro;
import com.google.common.base.Preconditions;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Macro evaluator used by batch application
 */
public class DefaultMacroEvaluator implements MacroEvaluator {
  private final WorkflowToken workflowToken;
  private final Map<String, String> runtimeArguments;
  private final long logicalStartTime;
  private final LogicalStartTimeMacro logicalStartTimeMacro;

  private static final String LOGICAL_START_TIME_FUNCTION_NAME = "logicalStartTime";
  private static final String SECURE_FUNCTION_NAME = "secure";

  public DefaultMacroEvaluator(WorkflowToken workflowToken, Map<String, String> runtimeArguments,
                               long logicalStartTime) {
    this.workflowToken = workflowToken;
    this.runtimeArguments = runtimeArguments;
    this.logicalStartTime = logicalStartTime;
    this.logicalStartTimeMacro = new LogicalStartTimeMacro();
  }

  @Override
  @Nullable
  public String lookup(String property) {
    // try workflow token
    Preconditions.checkNotNull(workflowToken, "Workflow token is null, you may not be running a workflow.");
    Value tokenValue = workflowToken.get(property);
    if (tokenValue != null) {
      return tokenValue.toString();
    }

    // try runtime arguments
    String runtimeArgumentMacro = runtimeArguments.get(property);
    if (runtimeArgumentMacro == null) {
      throw new InvalidMacroException(String.format("Macro '%s' not defined.", property));
    }
    return runtimeArgumentMacro;
  }

  @Override
  @Nullable
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    if (macroFunction.equals(LOGICAL_START_TIME_FUNCTION_NAME)) {
      return logicalStartTimeMacro.evaluate(logicalStartTime, arguments);
    } else if (macroFunction.equals(SECURE_FUNCTION_NAME)) {
      // todo lookup secure store, when available
      return "";
    }
    throw new InvalidMacroException(String.format("Unsupport macro function %s", macroFunction));
  }
}
