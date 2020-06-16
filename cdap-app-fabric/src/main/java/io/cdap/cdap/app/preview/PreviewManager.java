/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
package io.cdap.cdap.app.preview;

import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Interface used for managing the preview runs.
 */
public interface PreviewManager {

  /**
   * Start the preview of an application config provided as an input in a given namespace.
   * @param namespace the id of the namespace in which preview to be run
   * @param request the {@link AppRequest} with which preview need to be started
   * @return the {@link ApplicationId} assigned to the preview run
   * @throws Exception if there were any error during starting preview
   */
  ApplicationId start(NamespaceId namespace, AppRequest<?> request) throws Exception;

  /**
   * Get the {@link PreviewRunner} responsible for managing the preview.
   * @return the {@link PreviewRunner} associated with the preview
   */
  PreviewRunner getRunner();

  /**
   * Returns a {@link LogReader} for reading logs for the given preview.
   * @return the {@link LogReader} for reading logs for the given preview
   */
  LogReader getLogReader();
}
