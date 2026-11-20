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

package io.cdap.cdap.internal.app.runtime.schedule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.schedule.queue.Job;
import io.cdap.cdap.internal.app.runtime.schedule.queue.SimpleJob;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.ProgramStatusTrigger;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.TimeTrigger;
import io.cdap.cdap.internal.app.services.ProgramLifecycleService;
import io.cdap.cdap.internal.app.services.PropertiesResolver;
import io.cdap.cdap.internal.app.store.ApplicationMeta;
import io.cdap.cdap.metadata.FakePreferencesFetcher;
import io.cdap.cdap.metadata.PreferencesFetcher;
import io.cdap.cdap.proto.NamespaceConfig;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.security.AccessPermission;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.impersonation.NoOpOwnerAdmin;
import io.cdap.cdap.security.impersonation.OwnerAdmin;
import io.cdap.cdap.security.spi.authentication.SecurityRequestContext;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Tests {@link ScheduleTaskRunner}.
 */
public class ScheduleTaskRunnerTest {

  private static final Gson GSON = new Gson();

  @Test
  public void testRuntimeArgumentResolution() throws Exception {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      binder -> {
        binder.bind(OwnerAdmin.class).to(NoOpOwnerAdmin.class);
        binder.bind(NamespaceAdmin.class).to(InMemoryNamespaceAdmin.class);
        binder.bind(NamespaceQueryAdmin.class).to(InMemoryNamespaceAdmin.class);
        binder.bind(PreferencesFetcher.class)
          .toInstance(new FakePreferencesFetcher(Collections.singletonMap("key", "should-be-overridden")));
      });

    PropertiesResolver propertiesResolver = injector.getInstance(PropertiesResolver.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("app");
    ProgramSchedule programSchedule = new ProgramSchedule(
      "schedule", "desc", appId.workflow("wf2"),
      Collections.singletonMap("key", "val"), new ProgramStatusTrigger(appId.workflow("wf1")),
      Collections.emptyList());
    Map<String, String> userArgs = ScheduleTaskRunner.getUserArgs(programSchedule, propertiesResolver);
    Assert.assertEquals("val", userArgs.get("key"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testLaunchArgumentsOrderAndSystemArgProtection() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    ApplicationId appId = NamespaceId.DEFAULT.app("app");
    ProgramId workflowId = appId.workflow("wf");

    Store store = Mockito.mock(Store.class);
    DefaultApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "app", "1.0.0", "desc", null,
        new ArtifactId("app", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    Mockito.when(store.getLatest(appId.getAppReference()))
        .thenReturn(new ApplicationMeta("app", appSpec, null));
    Mockito.when(store.loadProgram(workflowId))
        .thenReturn(new ProgramDescriptor(workflowId, appSpec));

    PropertiesResolver propertiesResolver = Mockito.mock(PropertiesResolver.class);
    Mockito.when(propertiesResolver.getUserProperties(workflowId))
        .thenReturn(new HashMap<>(ImmutableMap.of("pref.key", "prefVal")));
    Mockito.when(propertiesResolver.getSystemProperties(workflowId))
        .thenReturn(new HashMap<>(ImmutableMap.of(
            ProgramOptionConstants.PRINCIPAL, "legitOwner/host@REALM",
            ProgramOptionConstants.APP_PRINCIPAL_EXISTS, "true",
            SystemArguments.NAMESPACE_CONFIG_PREFIX + "nsKey", "legitNsVal")));

    NamespaceQueryAdmin namespaceQueryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    Mockito.when(namespaceQueryAdmin.get(NamespaceId.DEFAULT))
        .thenReturn(new NamespaceMeta.Builder().setName(NamespaceId.DEFAULT).build());

    ProgramLifecycleService lifecycleService = Mockito.mock(ProgramLifecycleService.class);
    ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(
        store, lifecycleService, propertiesResolver, namespaceQueryAdmin, cConf);

    ProgramSchedule schedule = new ProgramSchedule(
        "sched1", "desc", workflowId,
        ImmutableMap.of("sched.key", "schedVal"),
        new TimeTrigger("0 * * * *"),
        Collections.emptyList());

    Map<String, String> notificationSystemOverrides = ImmutableMap.of(
        ProgramOptionConstants.CRON_EXPRESSION, "0 * * * *",
        ProgramOptionConstants.PRINCIPAL, "attacker/host@REALM",
        ProgramOptionConstants.APP_PRINCIPAL_EXISTS, "false",
        SystemArguments.NAMESPACE_CONFIG_PREFIX + "nsKey", "spoofedNsVal",
        SystemArguments.NAMESPACE_CONFIG_PREFIX + "injected", "spoofedExtra");
    Map<String, String> notificationUserOverrides = ImmutableMap.of(
        ProgramOptionConstants.LOGICAL_START_TIME, "1700000000000",
        "user.override.key", "fromNotification",
        SystemArguments.RUNTIME_PRINCIPAL_NAME, "runtimeUser/host@REALM");
    Notification notification = new Notification(
        Notification.Type.TIME,
        ImmutableMap.of(
            ProgramOptionConstants.SCHEDULE_NAME, "sched1",
            ProgramOptionConstants.SYSTEM_OVERRIDES, GSON.toJson(notificationSystemOverrides),
            ProgramOptionConstants.USER_OVERRIDES, GSON.toJson(notificationUserOverrides)));

    Job job = new SimpleJob(schedule, 0, 0L, ImmutableList.of(notification),
        Job.State.PENDING_LAUNCH, 0L);

    taskRunner.launch(job);

    ArgumentCaptor<Map<String, String>> userArgsCaptor = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<Map<String, String>> sysArgsCaptor = ArgumentCaptor.forClass(Map.class);
    Mockito.verify(lifecycleService).authorizePipelineRuntimeImpersonation(userArgsCaptor.capture());
    Mockito.verify(lifecycleService).runInternal(
        Mockito.eq(workflowId), userArgsCaptor.capture(), sysArgsCaptor.capture(), Mockito.eq(false));

    Map<String, String> capturedUserArgs = userArgsCaptor.getValue();
    Map<String, String> capturedSysArgs = sysArgsCaptor.getValue();

    // User args receive preferences, schedule properties, and notification USER_OVERRIDES (never SYSTEM_OVERRIDES)
    Assert.assertEquals("prefVal", capturedUserArgs.get("pref.key"));
    Assert.assertEquals("schedVal", capturedUserArgs.get("sched.key"));
    Assert.assertEquals("1700000000000",
        capturedUserArgs.get(ProgramOptionConstants.LOGICAL_START_TIME));
    Assert.assertEquals("fromNotification", capturedUserArgs.get("user.override.key"));
    Assert.assertEquals("runtimeUser/host@REALM",
        capturedUserArgs.get(SystemArguments.RUNTIME_PRINCIPAL_NAME));
    Assert.assertFalse(capturedUserArgs.containsKey(ProgramOptionConstants.CRON_EXPRESSION));

    // System args receive notification SYSTEM_OVERRIDES metadata, preserve authoritative PropertiesResolver keys,
    // strip injected namespace config keys, and never receive notification USER_OVERRIDES
    Assert.assertEquals("0 * * * *",
        capturedSysArgs.get(ProgramOptionConstants.CRON_EXPRESSION));
    Assert.assertEquals("legitOwner/host@REALM",
        capturedSysArgs.get(ProgramOptionConstants.PRINCIPAL));
    Assert.assertEquals("true",
        capturedSysArgs.get(ProgramOptionConstants.APP_PRINCIPAL_EXISTS));
    Assert.assertEquals("legitNsVal",
        capturedSysArgs.get(SystemArguments.NAMESPACE_CONFIG_PREFIX + "nsKey"));
    Assert.assertFalse(
        capturedSysArgs.containsKey(SystemArguments.NAMESPACE_CONFIG_PREFIX + "injected"));
    Assert.assertFalse(capturedSysArgs.containsKey("user.override.key"));
    Assert.assertFalse(capturedSysArgs.containsKey(SystemArguments.RUNTIME_PRINCIPAL_NAME));
  }

  @Test
  public void testLaunchRejectsUnauthorizedRuntimeImpersonationAndRestoresContext() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.KERBEROS_ENABLED, true);
    cConf.set(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL, "cdap/master@REALM");
    KerberosName.setRules("DEFAULT");

    ApplicationId appId = NamespaceId.DEFAULT.app("app");
    ProgramId workflowId = appId.workflow("wf");

    Store store = Mockito.mock(Store.class);
    DefaultApplicationSpecification appSpec = new DefaultApplicationSpecification(
        "app", "1.0.0", "desc", null,
        new ArtifactId("app", new ArtifactVersion("1.0.0"), ArtifactScope.USER),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    Mockito.when(store.getLatest(appId.getAppReference()))
        .thenReturn(new ApplicationMeta("app", appSpec, null));

    PropertiesResolver propertiesResolver = Mockito.mock(PropertiesResolver.class);
    Mockito.when(propertiesResolver.getUserProperties(workflowId))
        .thenReturn(new HashMap<>());
    Mockito.when(propertiesResolver.getSystemProperties(workflowId))
        .thenReturn(new HashMap<>(ImmutableMap.of(
            ProgramOptionConstants.PRINCIPAL, "appOwner/host@REALM",
            ProgramOptionConstants.APP_PRINCIPAL_EXISTS, "true")));

    NamespaceQueryAdmin namespaceQueryAdmin = Mockito.mock(NamespaceQueryAdmin.class);
    NamespaceMeta nsMeta = new NamespaceMeta.Builder()
        .setName(NamespaceId.DEFAULT)
        .setPrincipal("nsOwner/host@REALM")
        .build();
    Mockito.when(namespaceQueryAdmin.get(NamespaceId.DEFAULT)).thenReturn(nsMeta);

    ProgramLifecycleService lifecycleService = Mockito.mock(ProgramLifecycleService.class);
    AtomicReference<String> userIdDuringAuthCheck = new AtomicReference<>();
    Mockito.doAnswer(invocation -> {
      userIdDuringAuthCheck.set(SecurityRequestContext.getUserId());
      throw new UnauthorizedException(
          new Principal("appOwner", Principal.PrincipalType.USER),
          Collections.singleton(AccessPermission.IMPERSONATE),
          new KerberosPrincipalId("target/host@REALM"));
    }).when(lifecycleService).authorizePipelineRuntimeImpersonation(Mockito.anyMap());

    ScheduleTaskRunner taskRunner = new ScheduleTaskRunner(
        store, lifecycleService, propertiesResolver, namespaceQueryAdmin, cConf);

    ProgramSchedule schedule = new ProgramSchedule(
        "sched1", "desc", workflowId,
        ImmutableMap.of(SystemArguments.RUNTIME_PRINCIPAL_NAME, "target/host@REALM"),
        new TimeTrigger("0 * * * *"),
        Collections.emptyList());
    Job job = new SimpleJob(schedule, 0, 0L, Collections.emptyList(), Job.State.PENDING_LAUNCH, 0L);

    SecurityRequestContext.setUserId("callerBeforeSchedule");
    try {
      taskRunner.launch(job);
      Assert.fail("Expected UnauthorizedException when runtime principal impersonation is denied");
    } catch (UnauthorizedException expected) {
      // Expected
    }

    // Verify app owner principal took precedence over namespace principal during execute()
    Assert.assertEquals("appOwner", userIdDuringAuthCheck.get());
    // Verify original SecurityRequestContext userId was restored in finally
    Assert.assertEquals("callerBeforeSchedule", SecurityRequestContext.getUserId());
    // Verify runInternal was never invoked after authorization failure
    Mockito.verify(lifecycleService, Mockito.never())
        .runInternal(Mockito.any(), Mockito.anyMap(), Mockito.anyMap(), Mockito.anyBoolean());
  }
}
