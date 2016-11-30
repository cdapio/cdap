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
package co.cask.cdap.app.preview;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;

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
   * Get the {@link PreviewRunner} responsible for managing the given preview.
   * @param preview the application id of the preview for which {@link PreviewRunner} is to be returned
   * @return the {@link PreviewRunner} associted with the preview
   * @throws NotFoundException if the preview application is not found
   */
  PreviewRunner getRunner(ApplicationId preview) throws NotFoundException;
}
