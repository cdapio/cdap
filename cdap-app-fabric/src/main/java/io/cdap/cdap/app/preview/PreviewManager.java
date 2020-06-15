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

import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Interface used for managing the preview runs.
 */
public interface PreviewManager extends PreviewRunnerProvider {

  /**
   * Start the preview of an application config provided as an input in a given namespace.
   * @param namespace the id of the namespace in which preview to be run
   * @param request the {@link AppRequest} with which preview need to be started
   * @return the {@link ApplicationId} assigned to the preview run
   * @throws Exception if there were any error during starting preview
   */
  ApplicationId start(NamespaceId namespace, AppRequest<?> request) throws Exception;

  /**
   * Returns a {@link LogReader} for reading logs for the given preview.
   *
   * @param preview the application id of the preview for which {@link LogReader} is to be returned
   * @return the {@link LogReader} for reading logs for the given preview
   * @throws NotFoundException if the preview application is not found
   */
  LogReader getLogReader(ApplicationId preview) throws NotFoundException;
}
