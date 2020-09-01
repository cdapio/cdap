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

import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.internal.app.runtime.k8s.PreviewRequestPollerInfo;
import io.cdap.cdap.master.spi.twill.ExtendedTwillController;
import io.cdap.cdap.proto.id.ApplicationId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * A {@link PreviewRunStopper} implementation when preview runners is distributed to run in different processes.
 */
public class DistributedPreviewRunStopper implements PreviewRunStopper {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedPreviewRunStopper.class);
  private static final Gson GSON = new Gson();

  private final PreviewStore previewStore;
  private final TwillRunner twillRunner;

  @Inject
  DistributedPreviewRunStopper(PreviewStore previewStore, TwillRunner twillRunner) {
    this.previewStore = previewStore;
    this.twillRunner = twillRunner;
  }

  @Override
  public void stop(ApplicationId previewApp) throws Exception {
    byte[] info = previewStore.getPreviewRequestPollerInfo(previewApp);
    if (info == null) {
      // should not happen
      throw new IllegalStateException("Preview cannot be stopped. Please try stopping again or run the new preview.");
    }

    PreviewRequestPollerInfo pollerInfo = GSON.fromJson(new String(info, StandardCharsets.UTF_8),
                                                        PreviewRequestPollerInfo.class);
    Iterator<TwillController> controllers = twillRunner.lookup(PreviewRunnerTwillApplication.NAME).iterator();
    if (!controllers.hasNext()) {
      throw new IllegalStateException("Preview runners cannot be stopped. Please try again.");
    }

    LOG.debug("Stopping preview run {} with poller info {}", previewApp, pollerInfo);

    TwillController controller = controllers.next();
    Future<String> future;
    if (controller instanceof ExtendedTwillController) {
      future = ((ExtendedTwillController) controller).restartInstance(PreviewRunnerTwillRunnable.class.getSimpleName(),
                                                                      pollerInfo.getInstanceId(),
                                                                      pollerInfo.getInstanceUid());
    } else {
      future = controller.restartInstances(PreviewRunnerTwillRunnable.class.getSimpleName(),
                                           pollerInfo.getInstanceId());
    }
    future.get();
    LOG.info("Force stopped preview run {}", previewApp);
  }
}
