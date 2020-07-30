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

package io.cdap.cdap.master.environment.k8s;

import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.app.preview.PreviewConfigModule;
import io.cdap.cdap.app.store.preview.PreviewStore;
import io.cdap.cdap.data2.dataset2.lib.table.leveldb.LevelDBTableService;
import io.cdap.cdap.internal.app.preview.PreviewRunStopper;
import io.cdap.cdap.internal.app.runtime.k8s.PreviewRequestPollerInfo;
import io.cdap.cdap.internal.app.store.preview.DefaultPreviewStore;
import io.cdap.cdap.proto.id.ApplicationId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * Implementation of {@link PreviewRunStopper} in k8s environment.
 */
public class DistributedPreviewRunnerServiceStopper implements PreviewRunStopper {
  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(DistributedPreviewRunnerServiceStopper.class);
  private final PreviewStore previewStore;
  private final TwillRunnerService twillRunnerService;

  @Inject
  DistributedPreviewRunnerServiceStopper(
    @Named(PreviewConfigModule.PREVIEW_LEVEL_DB) LevelDBTableService previewLevelDBTableService,
    TwillRunnerService twillRunnerService) {
    this.previewStore = new DefaultPreviewStore(previewLevelDBTableService);
    this.twillRunnerService = twillRunnerService;
  }

  @Override
  public void stop(ApplicationId previewApp) throws Exception {
    byte[] pollerInfo = previewStore.getPreviewRequestPollerInfo(previewApp);
    if (pollerInfo == null) {
      // should not happen
      throw new IllegalStateException("Preview cannot be stopped. Please try stopping again or run the new preview.");
    }
    String pInfo = Bytes.toString(pollerInfo);
    PreviewRequestPollerInfo previewRequestPollerInfo = GSON.fromJson(pInfo, PreviewRequestPollerInfo.class);
    Iterator<TwillController> controllers = twillRunnerService.lookup("preview-runner").iterator();
    if (!controllers.hasNext()) {
      throw new IllegalStateException("Preview runners cannot be stopped. Please try again.");
    }
    TwillController controller = controllers.next();
    controller.restartInstances(previewRequestPollerInfo.getInstanceUid(), previewRequestPollerInfo.getInstanceId());
  }
}
