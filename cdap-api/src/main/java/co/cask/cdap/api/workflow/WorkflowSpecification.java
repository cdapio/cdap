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
package co.cask.cdap.api.workflow;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.common.PropertyProvider;

import java.util.List;
import java.util.Map;


/**
 * Specification for a {@link Workflow}
 *
 * <p>
 * Example WorkflowSpecification for a scheduled workflow:
 *
 *  <pre>
 *    <code>
 *      {@literal @}Override
 *      public void configure() {
 *        setName("PurchaseHistoryWorkflow");
 *        setDescription("PurchaseHistoryWorkflow description");
 *        addMapReduce("PurchaseHistoryBuilder");
 *        addSchedule("DailySchedule");
 *      }
 *    </code>
 *  </pre>
 *
 * See the Purchase example application.
 */
public interface WorkflowSpecification extends ProgramSpecification, PropertyProvider {
  /**
   *
   * @return list of schedules configured for the {@link Workflow}
   */
  List<String> getSchedules();

  /**
   *
   * @return list of {@link WorkflowActionEntry} configured to be executed by the {@link Workflow}
   */
  List<WorkflowActionEntry> getActions();

  /**
   *
   * @return map of custom actions configured for the {@link Workflow}
   */
  Map<String, WorkflowActionSpecification> getCustomActionMap();
}
