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

import io.cdap.cdap.common.service.AbstractRetryableScheduledService;
import io.cdap.cdap.common.service.RetryStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Service for polling {@link PreviewRequest} from {@link PreviewRequestQueue}
 */
public class PreviewRequestPollingService extends AbstractRetryableScheduledService {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewRequestPollingService.class);
  private final PreviewRequestPoller previewRequestPoller;
  private final PreviewRunnerProvider previewRunnerProvider;

  public PreviewRequestPollingService(PreviewRequestPoller previewRequestPoller,
                                      PreviewRunnerProvider previewRunnerProvider) {
    super(RetryStrategies.fixDelay(1, TimeUnit.SECONDS));
    this.previewRequestPoller = previewRequestPoller;
    this.previewRunnerProvider = previewRunnerProvider;
  }

  @Override
  protected long runTask() throws Exception {
    Optional<PreviewRequest> previewRequest = previewRequestPoller.poll();
    if (previewRequest.isPresent()) {
      PreviewRunner runner = previewRunnerProvider.getRunner(previewRequest.get().getProgram().getParent());
      try {
        runner.startPreview();
      } catch (Exception e) {
        LOG.warn("Failed to run preview.", e);
      }
    }
    return 1000L;
  }
}
