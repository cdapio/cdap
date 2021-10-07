/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.task;

import io.cdap.cdap.support.status.SupportBundleStatusTask;

import java.util.List;
import java.util.concurrent.Future;

/** Establishes an interface for support bundle task */
public interface SupportBundleTask {
  /** Collect Log or pipeline info */
  void initializeCollection() throws Exception;

  SupportBundleStatusTask initializeTask(String name, String type);
  void updateTask(SupportBundleStatusTask task, String basePath, String serviceName);
  void addToStatus(String basePath);
  void completeProcessing(List<Future> futureListm, String basePath, String serviceName);
  void queueTaskAfterFailed(String serviceName, SupportBundleStatusTask task);
}
