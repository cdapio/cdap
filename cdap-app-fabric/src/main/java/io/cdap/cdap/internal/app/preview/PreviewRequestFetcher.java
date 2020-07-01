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

package io.cdap.cdap.internal.app.preview;

import io.cdap.cdap.app.preview.PreviewRequest;

import java.io.IOException;
import java.util.Optional;

/**
 * An interface to define fetching of {@link PreviewRequest}.
 */
public interface PreviewRequestFetcher {

  /**
   * Fetches a {@link PreviewRequest} for preview execution
   *
   * @return an {@link Optional} of {@link PreviewRequest} or an empty {@link Optional} if there is no preview request
   *         available
   * @throws IOException if failed to fetch preview request
   */
  Optional<PreviewRequest> fetch() throws IOException;
}
