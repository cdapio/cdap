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

package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.NodeValue;
import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.api.workflow.WorkflowToken;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of the {@link WorkflowToken} that delegates the get operations to
 * the {@link BasicWorkflowToken} passed to it as a parameter in the constructor.
 * Put operation throws {@link UnsupportedOperationException}.
 */
public class NonUpdatableWorkflowToken implements WorkflowToken {
  private final WorkflowToken delegate;

  public NonUpdatableWorkflowToken(WorkflowToken delegate) {
    this.delegate = delegate;
  }

  @Override
  public void put(String key, String value) {
    throw new UnsupportedOperationException("Put operation is not allowed from the Mapper and Reducer classes.");
  }

  @Override
  public void put(String key, Value value) {
    throw new UnsupportedOperationException("Put operation is not allowed from the Mapper and Reducer classes.");
  }

  @Nullable
  @Override
  public Value get(String key) {
    return delegate.get(key);
  }

  @Nullable
  @Override
  public Value get(String key, Scope scope) {
    return delegate.get(key, scope);
  }

  @Nullable
  @Override
  public Value get(String key, String nodeName) {
    return delegate.get(key, nodeName);
  }

  @Nullable
  @Override
  public Value get(String key, String nodeName, Scope scope) {
    return delegate.get(key, nodeName, scope);
  }

  @Override
  public List<NodeValue> getAll(String key) {
    return delegate.getAll(key);
  }

  @Override
  public List<NodeValue> getAll(String key, Scope scope) {
    return delegate.getAll(key, scope);
  }

  @Override
  public Map<String, Value> getAllFromNode(String nodeName) {
    return delegate.getAllFromNode(nodeName);
  }

  @Override
  public Map<String, Value> getAllFromNode(String nodeName, Scope scope) {
    return delegate.getAllFromNode(nodeName, scope);
  }

  @Override
  public Map<String, List<NodeValue>> getAll() {
    return delegate.getAll();
  }

  @Override
  public Map<String, List<NodeValue>> getAll(Scope scope) {
    return delegate.getAll(scope);
  }

  @Override
  public Map<String, Map<String, Long>> getMapReduceCounters() {
    return delegate.getMapReduceCounters();
  }
}
