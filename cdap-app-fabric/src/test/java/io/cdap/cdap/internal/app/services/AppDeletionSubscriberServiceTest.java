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

package io.cdap.cdap.internal.app.services;

import com.google.inject.Injector;
import io.cdap.cdap.ConfigTestApp;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.app.deploy.AppDeletionPublisher;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.service.RetryStrategyType;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/**
 * Unit test for {@link io.cdap.cdap.metadata.AppDeletionSubscriberService} and corresponding writers.
 */
public class AppDeletionSubscriberServiceTest extends AppFabricTestBase {
  private Injector injector = getInjector();
  private MessagingService messagingService = injector.getInstance(MessagingService.class);
  private MultiThreadMessagingContext messagingContext = new MultiThreadMessagingContext(messagingService);
  private AppDeletionPublisher publisher = new AppDeletionPublisher(cConf, messagingContext);
  private Store store = injector.getInstance(DefaultStore.class);

  private static CConfiguration cConf;
  // Number of versions for the app to be deleted
  private static final int TEST_APP_SIZE = 5;

  @BeforeClass
  public static void beforeClass() throws Throwable {
    cConf = createBasicCConf();
    initializeAndStartServices(cConf);
    // use a fast retry strategy with not too many retries, to speed up the test
    String prefix = "app.delete.event.";
    cConf.set(prefix + Constants.Retry.TYPE, RetryStrategyType.FIXED_DELAY.toString());
    cConf.set(prefix + Constants.Retry.MAX_RETRIES, "100");
    cConf.set(prefix + Constants.Retry.MAX_TIME_SECS, "10");
    cConf.set(prefix + Constants.Retry.DELAY_BASE_MS, "200");
    cConf.set(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT, "20");
  }

  private void createApp(ApplicationId appId) throws Exception {
    // create an app with more than one version - the calling of app deletion should delete all the app versions
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "configapp", "1.0.0");
    addAppArtifact(artifactId, ConfigTestApp.class);
    ConfigTestApp.ConfigClass config = new ConfigTestApp.ConfigClass("abc", "def");
    AppRequest<ConfigTestApp.ConfigClass> request = new AppRequest<>(
      new ArtifactSummary(artifactId.getName(), artifactId.getVersion().getVersion()), config);
    for (int i = 0; i < TEST_APP_SIZE; i++) {
      appId = new ApplicationId(Id.Namespace.DEFAULT.getId(), "testDelete", "version_" + i);
      Assert.assertEquals(200, deploy(appId, request).getResponseCode());
    }
  }

  @Test
  public void testAppDeletionPublishMessage() throws Exception {
    ApplicationId appId = new ApplicationId(Id.Namespace.DEFAULT.getId(), "testDelete");
    createApp(appId);
    // mark the app as to be deleted
    store.markDeleteApplication(appId);
    // publish app deletion message
    publisher.publishAppDeletionEvent(appId);
    // Verify all the versions of the app are removed
    ApplicationReference appReference = appId.getAppReference();
    Tasks.waitFor(true, () -> store.getAllAppVersionsAppIds(appReference).isEmpty(),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }

  @Test
  public void testAppDeletionPublishMessageUnmarkedApp() throws Exception {
    ApplicationId appId = new ApplicationId(Id.Namespace.DEFAULT.getId(), "testDelete");
    createApp(appId);
    store.getApplication(appId);
    // publish app deletion message
    publisher.publishAppDeletionEvent(appId);
    // Verify all the versions of the app are removed
    ApplicationReference appReference = appId.getAppReference();
    // App is not marked for deletion, as such should not be removed
    Tasks.waitFor(TEST_APP_SIZE, () -> store.getAllAppVersionsAppIds(appReference).size(),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);
  }
}
