/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.sourcecontrol;

import com.google.inject.Inject;
import io.cdap.cdap.common.ApplicationNotFoundException;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.internal.app.services.ApplicationLifecycleService;
import io.cdap.cdap.metadata.LocalApplicationDetailFetcher;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.app.AppVersion;
import io.cdap.cdap.proto.app.MarkLatestAppsRequest;
import io.cdap.cdap.proto.app.UpdateMultiSourceControlMetaReqeust;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import java.io.IOException;
import java.time.Clock;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Local implementation of {@link ApplicationManager} to fetch/update data while running in
 * app-fabric.
 */
public class LocalApplicationManager implements ApplicationManager {

  private final ApplicationLifecycleService appLifeCycleService;
  private final LocalApplicationDetailFetcher appDetailsFetcher;
  private final Clock clock;

  private static final Logger LOG = LoggerFactory.getLogger(LocalApplicationManager.class);


  @Inject
  LocalApplicationManager(ApplicationLifecycleService appLifeCycleService,
      LocalApplicationDetailFetcher fetcher) {
    this(appLifeCycleService, fetcher, Clock.systemUTC());
  }

  LocalApplicationManager(ApplicationLifecycleService appLifeCycleService,
      LocalApplicationDetailFetcher fetcher, Clock clock) {
    this.appLifeCycleService = appLifeCycleService;
    this.appDetailsFetcher = fetcher;
    this.clock = clock;
  }


  @Override
  public ApplicationId deployApp(ApplicationReference appRef, PullAppResponse<?> pullDetails)
      throws Exception {
    String versionId = RunIds.generate().getId();
    ApplicationId appId = appRef.app(versionId);

    AppRequest<?> appRequest = pullDetails.getAppRequest();
    SourceControlMeta sourceControlMeta = new SourceControlMeta(
        pullDetails.getApplicationFileHash(), pullDetails.getCommitId(), clock.instant());

    LOG.info("Start to deploy app {} in namespace {} without marking latest",
        appId.getApplication(), appId.getParent());

    appLifeCycleService.deployApp(appId, appRequest, sourceControlMeta, x -> {
    }, true);
    return appId;
  }

  @Override
  public void markAppVersionsLatest(NamespaceId namespace, List<AppVersion> apps)
      throws SourceControlException, ApplicationNotFoundException, BadRequestException, IOException {
    MarkLatestAppsRequest request = new MarkLatestAppsRequest(apps);
    LOG.info("Marking latest in namespace {} : {}", namespace, apps);
      appLifeCycleService.markAppsAsLatest(namespace, request);
  }

  @Override
  public void updateSourceControlMeta(NamespaceId namespace,
      UpdateMultiSourceControlMetaReqeust metas)
      throws SourceControlException, BadRequestException, IOException {
      appLifeCycleService.updateSourceControlMeta(namespace, metas);
  }

  @Override
  public ApplicationDetail get(ApplicationReference appRef)
      throws IOException, NotFoundException, UnauthorizedException {
    return appDetailsFetcher.get(appRef);
  }

}


