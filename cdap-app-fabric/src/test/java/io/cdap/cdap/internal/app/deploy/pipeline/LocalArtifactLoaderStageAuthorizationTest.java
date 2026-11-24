/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.deploy.pipeline;

import com.google.common.util.concurrent.Futures;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.deploy.ConfigResponse;
import io.cdap.cdap.app.deploy.Configurator;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.deploy.ConfiguratorFactory;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.pipeline.StageContext;
import io.cdap.cdap.internal.schedule.ScheduleCreationSpec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.Authorizable;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.cdap.security.authorization.InMemoryAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Authorization tests for schedule triggers checked during {@link LocalArtifactLoaderStage}.
 */
public class LocalArtifactLoaderStageAuthorizationTest {

  private static final NamespaceId NS = new NamespaceId("deployns");
  private static final ApplicationId APP_ID = NS.app("deployedApp");
  private static final Principal USER =
      new Principal("deploy-user", Principal.PrincipalType.USER);

  @Test
  public void testCrossAppTriggerDeniedBeforeEmit() throws Exception {
    ProgramId hiddenUpstream = NS.app("hiddenUpstream").workflow("wf");
    Map<String, ScheduleCreationSpec> schedules = Collections.singletonMap("s1",
        scheduleSpec("s1", hiddenUpstream));
    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(APP_ID), USER,
        Collections.singleton(StandardPermission.CREATE));
    StageContext context = new StageContext(mockDeploymentInfo(APP_ID));
    LocalArtifactLoaderStage stage = createStage(accessController, schedules);

    AuthenticationTestContext.actAsPrincipal(USER);
    try {
      stage.process(context);
      Assert.fail("Expected UnauthorizedException");
    } catch (UnauthorizedException e) {
      // expected
    }
    Assert.assertNull(context.getDownStream());
  }

  @Test
  public void testSameAppAndAuthorizedTriggerAllowed() throws Exception {
    ProgramId sameAppProgram = APP_ID.workflow("otherWf");
    ProgramId visibleUpstream = NS.app("visibleUpstream").workflow("wf");
    Map<String, ScheduleCreationSpec> schedules = new LinkedHashMap<>();
    schedules.put("sameApp", scheduleSpec("sameApp", sameAppProgram));
    schedules.put("crossApp", scheduleSpec("crossApp", visibleUpstream));

    InMemoryAccessController accessController = new InMemoryAccessController();
    accessController.grant(Authorizable.fromEntityId(APP_ID), USER,
        Collections.singleton(StandardPermission.CREATE));
    accessController.grant(Authorizable.fromEntityId(visibleUpstream), USER,
        Collections.singleton(StandardPermission.GET));

    StageContext context = new StageContext(mockDeploymentInfo(APP_ID));
    LocalArtifactLoaderStage stage = createStage(accessController, schedules);

    AuthenticationTestContext.actAsPrincipal(USER);
    stage.process(context);
    Assert.assertTrue(context.getDownStream() instanceof ApplicationDeployable);
  }

  private static LocalArtifactLoaderStage createStage(InMemoryAccessController accessController,
      Map<String, ScheduleCreationSpec> schedules) {
    ApplicationSpecification appSpec = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(appSpec.getName()).thenReturn(APP_ID.getApplication());
    Mockito.when(appSpec.getProgramSchedules()).thenReturn(schedules);

    ConfigResponse response = Mockito.mock(ConfigResponse.class);
    Mockito.when(response.getExitCode()).thenReturn(0);
    Mockito.when(response.getAppSpecInfo()).thenReturn(
        new AppSpecInfo(appSpec, Collections.emptyList(), Collections.emptyMap()));

    Configurator configurator = Mockito.mock(Configurator.class);
    Mockito.when(configurator.config()).thenReturn(Futures.immediateFuture(response));

    ConfiguratorFactory configuratorFactory = Mockito.mock(ConfiguratorFactory.class);
    Mockito.when(configuratorFactory.create(Mockito.any())).thenReturn(configurator);

    return new LocalArtifactLoaderStage(CConfiguration.create(), Mockito.mock(Store.class),
        accessController, new AuthenticationTestContext(),
        Mockito.mock(CapabilityReader.class), configuratorFactory);
  }

  private static AppDeploymentInfo mockDeploymentInfo(ApplicationId appId) {
    AppDeploymentInfo info = Mockito.mock(AppDeploymentInfo.class);
    Mockito.when(info.getNamespaceId()).thenReturn(appId.getNamespaceId());
    Mockito.when(info.getApplicationName()).thenReturn(appId.getApplication());
    Mockito.when(info.getApplicationVersion()).thenReturn(appId.getVersion());
    return info;
  }

  private static ScheduleCreationSpec scheduleSpec(String name, ProgramId upstream) {
    return new ScheduleCreationSpec(name, "", "targetWf", Collections.emptyMap(),
        new ProgramStatusTrigger(upstream, ProgramStatus.COMPLETED), Collections.emptyList(), 0L);
  }
}
