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

package io.cdap.cdap.api.customaction;

import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.RuntimeContext;
import io.cdap.cdap.api.SchedulableProgramContext;
import io.cdap.cdap.api.ServiceDiscoverer;
import io.cdap.cdap.api.Transactional;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.lineage.field.LineageRecorder;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metadata.MetadataWriter;
import io.cdap.cdap.api.plugin.PluginContext;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.workflow.WorkflowInfoProvider;

/**
 * Represents runtime context of the {@link CustomAction} in the Workflow.
 */
public interface CustomActionContext extends SchedulableProgramContext, RuntimeContext, DatasetContext, Transactional,
  WorkflowInfoProvider, PluginContext, SecureStore, ServiceDiscoverer, MessagingContext, MetadataReader,
  MetadataWriter, LineageRecorder {

  /**
   * Return the specification of the custom action.
   */
  CustomActionSpecification getSpecification();

  /**
   * Returns the logical start time of the batch job which triggers this instance of an action.
   * Logical start time is the time when the triggering Batch job is supposed to start if it is started
   * by the scheduler. Otherwise it would be the current time when the action runs.
   */
  long getLogicalStartTime();

  /**
   * Return the state of the custom action.
   */
  ProgramState getState();
}
