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

package co.cask.cdap.datapipeline;

import co.cask.cdap.api.customaction.CustomAction;
import co.cask.cdap.api.workflow.Condition;
import co.cask.cdap.api.workflow.WorkflowForkConfigurer;

/**
 * Implementation of the {@link WorkflowProgramAdder} which adds nodes on the Fork whose parent is Condition.
 * @param <T> type of the current configurer
 */
public class ForkToConditionAdder<T extends WorkflowForkConfigurer> implements WorkflowProgramAdder {

  private final WorkflowProgramAdder parent;
  private final T configurer;

  public ForkToConditionAdder(WorkflowProgramAdder parent, T configurer) {
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
    return new ConditionToForkAdder<>(this, configurer.condition(condition));
  }

  @Override
  public WorkflowProgramAdder otherwise() {
    throw new UnsupportedOperationException("Operation not supported on the Fork.");
  }

  @Override
  public WorkflowProgramAdder end() {
    throw new UnsupportedOperationException("Operation not supported on the Fork.");
  }

  @Override
  public WorkflowProgramAdder fork() {
    return new ForkToForkAdder<>(this, configurer.fork());
  }

  @Override
  public WorkflowProgramAdder also() {
    configurer.also();
    return this;
  }

  @Override
  public WorkflowProgramAdder join() {
    configurer.join();
    return parent;
  }
}
