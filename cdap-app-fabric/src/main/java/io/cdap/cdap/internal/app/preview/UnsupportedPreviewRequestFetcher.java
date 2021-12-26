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

package io.cdap.cdap.internal.app.preview;

import com.google.inject.Inject;
import io.cdap.cdap.app.preview.PreviewRequest;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;

import java.io.IOException;
import java.util.Optional;

/**
 * Fetch preview requests from remote server.
 */
public class UnsupportedPreviewRequestFetcher implements PreviewRequestFetcher {
  @Inject
  public UnsupportedPreviewRequestFetcher() {
  }

  @Override
  public Optional<PreviewRequest> fetch() throws IOException, UnauthorizedException {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public Optional<PreviewRequest> fetch(PreviewRequestPollerInfoProvider pollerInfoProvider)
    throws IOException, UnauthorizedException {
    throw new UnsupportedOperationException("unsupported");
  }
}
