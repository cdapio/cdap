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

package co.cask.cdap.api.workflow;

import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.mapreduce.MapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;

import java.util.Map;

/**
 * The abstract class provides default implementation of the {@link Condition} methods for an easy extension
 */
public abstract class AbstractCondition implements Condition {

  private String name;
  private ConditionConfigurer configurer;
  private WorkflowContext context;

  protected AbstractCondition() {
    // no-op, for instantiation only
  }

  protected AbstractCondition(String name) {
    this.name = name;
  }

  protected void configure() {

  }

  @Override
  public void configure(ConditionConfigurer configurer) {
    this.configurer = configurer;
    setName(name == null ? getClass().getSimpleName() : name);
    configure();
  }

  protected void setName(String name) {
    configurer.setName(name);
  }

  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public void initialize(WorkflowContext context) throws Exception {
    this.context = context;
  }

  /**
   * Classes derived from {@link AbstractCondition} can override this method to destroy the {@link Condition}.
   */
  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  public void destroy() {

  }

  /**
   * @return an instance of {@link WorkflowContext}
   */
  protected final WorkflowContext getContext() {
    return context;
  }
}
