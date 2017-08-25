/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.api.RuntimeContext;
import co.cask.cdap.api.workflow.WorkflowInfoProvider;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.etl.api.action.SettableArguments;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Default implementation of {@link SettableArguments}.
 */
public class BasicArguments implements SettableArguments, Serializable {

  private final Map<String, String> options;
  private final Map<String, String> addedArguments;

  public BasicArguments(Map<String, String> arguments) {
    this(null, arguments);
  }

  public <T extends WorkflowInfoProvider & RuntimeContext> BasicArguments(T context) {
    this(context.getWorkflowToken(), context.getRuntimeArguments());
  }

  public BasicArguments(@Nullable WorkflowToken token, Map<String, String> runtimeArguments) {
    options = new HashMap<>();
    options.putAll(runtimeArguments);
    // not expected, but can happen if user runs just the program and not the workflow
    if (token != null) {
      for (String tokenKey : token.getAll(WorkflowToken.Scope.USER).keySet()) {
        options.put(tokenKey, token.get(tokenKey, WorkflowToken.Scope.USER).toString());
      }
    }
    addedArguments = new HashMap<>();
  }

  @Override
  public boolean has(String name) {
    return options.containsKey(name);
  }

  @Override
  public String get(String name) {
    return options.get(name);
  }

  @Override
  public void set(String name, String value) {
    options.put(name, value);
    addedArguments.put(name, value);
  }

  @Override
  public Map<String, String> asMap() {
    return Collections.unmodifiableMap(options);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return options.entrySet().iterator();
  }

  public Map<String, String> getAddedArguments() {
    return Collections.unmodifiableMap(addedArguments);
  }
}
