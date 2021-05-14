/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.api.service.http;

import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.macro.MacroParserOptions;
import io.cdap.cdap.api.service.worker.RunnableTaskRequest;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

import java.io.IOException;
import java.util.Map;

/**
 * A System HttpServiceContext that exposes capabilities beyond those available to service contexts for user services.
 */
@Beta
public interface SystemHttpServiceContext extends HttpServiceContext, TransactionRunner {

  /**
   * Evaluates lookup macros and the 'secure' macro function using provided macro evaluator.
   *
   * @param namespace  namespace in which macros needs to be evaluated
   * @param properties key-value map of properties to evaluate
   * @param evaluator  macro evaluator to be used to evaluate macros
   * @return map of evaluated macros
   * @throws InvalidMacroException indicates that there is an invalid macro
   */
  default Map<String, String> evaluateMacros(String namespace, Map<String, String> properties,
                                             MacroEvaluator evaluator) throws InvalidMacroException {
    return evaluateMacros(namespace, properties, evaluator,
                          MacroParserOptions.builder()
                            .setFunctionWhitelist("secure")
                            .setEscaping(false)
                            .build());
  }

  /**
   * Evaluates macros using provided macro evaluator with the provided parsing options.
   *
   * @param namespace  namespace in which macros needs to be evaluated
   * @param properties key-value map of properties to evaluate
   * @param evaluator  macro evaluator to be used to evaluate macros
   * @param options    macro parsing options
   * @return map of evaluated macros
   * @throws InvalidMacroException indicates that there is an invalid macro
   */
  Map<String, String> evaluateMacros(String namespace, Map<String, String> properties,
                                     MacroEvaluator evaluator, MacroParserOptions options) throws InvalidMacroException;

  /**
   * Get preferences for the given namespace.
   * <p>
   * This method fetches preferences for the supplied namespace when the method is unvoked, unlike {@link
   * io.cdap.cdap.api.RuntimeContext#getRuntimeArguments()} which returns arguments at the time the context was
   * created.
   * <p>
   * This might be a network call, depending on the underlying implementation.
   *
   * @param namespace the name of the namespace to fetch preferences for.
   * @param resolved  true if resolved properties are desired.
   * @return Map containing Preferences keys and values.
   * @throws IOException is the preferences for the supplied namespace could not be fetched.
   * @throws IllegalArgumentException if the namespace doesn't exist.
   */
  default Map<String, String> getPreferencesForNamespace(String namespace, boolean resolved)
    throws IOException, IllegalArgumentException {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Runs the task from {@link io.cdap.cdap.api.service.worker.RunnableTaskRequest} remotely on a task worker
   * @param runnableTaskRequest Details of the task
   * @return byte[] result
   * @throws IOException if there is a problem in connecting to remote worker
   */
  byte[] runTask(RunnableTaskRequest runnableTaskRequest) throws IOException;
}
