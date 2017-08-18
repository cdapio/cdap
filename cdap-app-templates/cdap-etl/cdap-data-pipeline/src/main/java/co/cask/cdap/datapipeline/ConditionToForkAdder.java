/*
 * Copyright © 201& Cask Data, Inc.
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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.workflow.Condition;
import co.cask.cdap.api.workflow.WorkflowConditionConfigurer;

/**
 * Implementation of {@link WorkflowProgramAdder} which adds node to the branch of the Condition whose parent is Fork
 * in the Workflow.
 * @param <T> type of the current configurer
 */
public class ConditionToForkAdder<T extends WorkflowConditionConfigurer> implements WorkflowProgramAdder {

  private final WorkflowProgramAdder parent;
  private final T configurer;

  public ConditionToForkAdder(WorkflowProgramAdder parent, T configurer) {
    this.parent = parent;
    this.configurer = configurer;
  }

  @Override
  public void addMapReduce(String name) {
    configurer.addMapReduce(name);
  }

  @Override
  public void addSpark(String name) {
    configurer.addSpark(name);
  }

  @Override
  public void addAction(CustomAction action) {
    configurer.addAction(action);
  }

  @Override
  public WorkflowProgramAdder condition(Condition condition) {
    return new ConditionToConditionAdder<>(this, configurer.condition(condition));
  }

  @Override
  public WorkflowProgramAdder otherwise() {
    configurer.otherwise();
    return this;
  }

  @Override
  public WorkflowProgramAdder end() {
    configurer.end();
    return parent;
  }

  @Override
  public WorkflowProgramAdder fork() {
    return new ForkToConditionAdder<>(this, configurer.fork());
  }

  @Override
  public WorkflowProgramAdder also() {
    throw new UnsupportedOperationException("Operation not supported on the Condition");
  }

  @Override
  public WorkflowProgramAdder join() {
    throw new UnsupportedOperationException("Operation not supported on the Condition");
  }
}
