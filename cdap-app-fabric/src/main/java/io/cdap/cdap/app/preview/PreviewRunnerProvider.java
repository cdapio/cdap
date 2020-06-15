/*
 * Copyright © 2020 Cask Data, Inc.
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
import io.cdap.cdap.proto.id.ApplicationId;

/**
 * Provides {@link PreviewRunner}.
 */
public interface PreviewRunnerProvider {
  /**
   * Get the {@link PreviewRunner} responsible for managing the given preview.
   * @param preview the application id of the preview for which {@link PreviewRunner} is to be returned
   * @return the {@link PreviewRunner} associted with the preview
   * @throws NotFoundException if the preview application is not found
   */
  PreviewRunner getRunner(ApplicationId preview) throws NotFoundException;
}
