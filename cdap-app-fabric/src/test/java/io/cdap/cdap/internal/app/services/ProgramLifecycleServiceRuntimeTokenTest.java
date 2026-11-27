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

package io.cdap.cdap.internal.app.services;

import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.Plugin;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.api.schedule.TriggeringScheduleInfo;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.config.PreferencesService;
import io.cdap.cdap.features.Feature;
import io.cdap.cdap.internal.app.DefaultApplicationSpecification;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.internal.app.runtime.monitor.RuntimeScopes;
import io.cdap.cdap.internal.app.runtime.plugin.ConnectionPropertiesFetcher;
import io.cdap.cdap.internal.app.runtime.schedule.DefaultTriggeringScheduleInfo;
import io.cdap.cdap.internal.app.runtime.schedule.TriggeringScheduleInfoAdapter;
import io.cdap.cdap.internal.app.runtime.schedule.trigger.DefaultProgramStatusTriggerInfo;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.capability.CapabilityReader;
import io.cdap.cdap.internal.profile.ProfileService;
import io.cdap.cdap.internal.provision.ProvisionerNotifier;
import io.cdap.cdap.internal.provision.ProvisioningService;
import io.cdap.cdap.internal.tethering.runtime.spi.provisioner.TetheringProvisioner;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProfileId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.profile.Profile;
import io.cdap.cdap.proto.provisioner.ProvisionerDetail;
import io.cdap.cdap.proto.provisioner.ProvisionerInfo;
import io.cdap.cdap.runtime.spi.RuntimeMonitorType;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for runtime scope computation during {@link ProgramLifecycleService#runInternal}.
 */
public class ProgramLifecycleServiceRuntimeTokenTest {

  private static final Gson SCHEDULE_GSON =
      TriggeringScheduleInfoAdapter.addTypeAdapters(new GsonBuilder()).create();
  private static final NamespaceId NS = new NamespaceId("ns1");
  private static final ProgramId WORKFLOW_ID = NS.app("pipe1", "1.0").workflow("DataPipelineWorkflow");
  private static final ArtifactId SYSTEM_ARTIFACT =
      new ArtifactId("cdap-data-pipeline", new ArtifactVersion("6.12.0"), ArtifactScope.SYSTEM);
  private static final ProfileId DATAPROC_PROFILE_ID = NS.profile("dp-prof");
  private static final ProfileId SYSTEM_DATAPROC_PROFILE_ID = NamespaceId.SYSTEM.profile("dataproc");
  private static final ProfileId TETHERING_PROFILE_ID = NS.profile("tether-prof");

  private CConfiguration cConf;
  private Store store;
  private ProfileService profileService;
  private ProvisioningService provisioningService;
  private ProvisionerNotifier provisionerNotifier;
  private FlowControlService flowControlService;
  private ArtifactRepository artifactRepository;
  private ConnectionPropertiesFetcher connectionPropertiesFetcher;
  private ProgramLifecycleService lifecycleService;

  @Before
  public void setUp() throws Exception {
    cConf = CConfiguration.create();
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, true);
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    cConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc",
        RuntimeMonitorType.URL.name());
    cConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + TetheringProvisioner.TETHERING_NAME,
        RuntimeMonitorType.URL.name());

    store = Mockito.mock(Store.class);
    profileService = Mockito.mock(ProfileService.class);
    provisioningService = Mockito.mock(ProvisioningService.class);
    provisionerNotifier = Mockito.mock(ProvisionerNotifier.class);
    flowControlService = Mockito.mock(FlowControlService.class);
    connectionPropertiesFetcher = Mockito.mock(ConnectionPropertiesFetcher.class);

    FlowControlService.Counter zeroCounter = Mockito.mock(FlowControlService.Counter.class);
    Mockito.when(zeroCounter.getLaunchingCount()).thenReturn(0);
    Mockito.when(zeroCounter.getRunningCount()).thenReturn(0);
    Mockito.when(flowControlService.addRequestAndGetCounter(
        Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(zeroCounter);

    Profile dataprocProfile = new Profile("dp-prof", "Dataproc", "desc",
        new ProvisionerInfo("gcp-dataproc", Collections.emptyList()));
    Profile systemDataprocProfile = new Profile("dataproc", "System Dataproc", "desc",
        new ProvisionerInfo("gcp-dataproc", Collections.emptyList()));
    Profile nativeProfile = Profile.NATIVE;
    Profile tetheringProfile = new Profile("tether-prof", "Tether", "desc",
        new ProvisionerInfo(TetheringProvisioner.TETHERING_NAME, Collections.emptyList()));

    Mockito.when(profileService.getProfile(Mockito.eq(DATAPROC_PROFILE_ID), Mockito.anyMap()))
        .thenReturn(dataprocProfile);
    Mockito.when(profileService.getProfile(Mockito.eq(SYSTEM_DATAPROC_PROFILE_ID), Mockito.anyMap()))
        .thenReturn(systemDataprocProfile);
    Mockito.when(profileService.getProfile(Mockito.eq(ProfileId.NATIVE), Mockito.anyMap()))
        .thenReturn(nativeProfile);
    Mockito.when(profileService.getProfile(Mockito.eq(TETHERING_PROFILE_ID), Mockito.anyMap()))
        .thenReturn(tetheringProfile);

    Mockito.when(provisioningService.getProvisionerDetail("gcp-dataproc"))
        .thenReturn(new ProvisionerDetail("gcp-dataproc", "Dataproc", "desc",
            Collections.emptyList(), null, null, false));
    Mockito.when(provisioningService.getProvisionerDetail(Profile.NATIVE.getProvisioner().getName()))
        .thenReturn(new ProvisionerDetail(Profile.NATIVE.getProvisioner().getName(), "Native",
            "desc", Collections.emptyList(), null, null, false));
    Mockito.when(provisioningService.getProvisionerDetail(TetheringProvisioner.TETHERING_NAME))
        .thenReturn(new ProvisionerDetail(TetheringProvisioner.TETHERING_NAME, "Tethering",
            "desc", Collections.emptyList(), null, null, false));

    Mockito.when(connectionPropertiesFetcher.forNamespace(NS.getNamespace()))
        .thenReturn(name -> Optional.empty());

    artifactRepository = Mockito.mock(ArtifactRepository.class);
    ArtifactDetail emptyArtifactDetail = new ArtifactDetail(
        new ArtifactDescriptor(NS.getNamespace(), SYSTEM_ARTIFACT, URI.create("file:///tmp/a.jar")),
        new ArtifactMeta(ArtifactClasses.builder().build()));
    Mockito.when(artifactRepository.getArtifact(Mockito.any())).thenReturn(emptyArtifactDetail);

    lifecycleService = newLifecycleService(connectionPropertiesFetcher);
  }

  private ProgramLifecycleService newLifecycleService(ConnectionPropertiesFetcher fetcher) {
    return new ProgramLifecycleService(
        cConf,
        store,
        profileService,
        Mockito.mock(PropertiesResolver.class),
        Mockito.mock(PreferencesService.class),
        Mockito.mock(AccessEnforcer.class),
        Mockito.mock(AuthenticationContext.class),
        provisionerNotifier,
        provisioningService,
        Mockito.mock(ProgramStateWriter.class),
        Mockito.mock(CapabilityReader.class),
        artifactRepository,
        flowControlService,
        fetcher);
  }

  @Test
  public void testDefaultIssuanceOnComputesScopesFromResolvedArgs() throws Exception {
    // Pin cdap-default.xml default value directly
    Assert.assertEquals("true", cConf.get(Constants.RuntimeMonitor.RUNTIME_TOKEN_ENABLED));

    ApplicationSpecification appSpec = appSpecWithProp("${secure(s-${env})}");
    stubApp(appSpec);

    Map<String, String> userArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName(),
        "env", "unscoped",
        "cluster.mycluster.env", "fromCluster",
        "app.pipe1.env", "fromApp");
    Map<String, String> sysArgs = Collections.singletonMap(Constants.CLUSTER_NAME, "mycluster");

    lifecycleService.runInternal(WORKFLOW_ID, userArgs, sysArgs, true);

    ProgramOptions flowOptions = captureFlowControlOptions();
    ProgramOptions provOptions = captureProvisioningOptions();

    String rawScopes = provOptions.getArguments().getOption(ProgramOptionConstants.RUNTIME_SCOPES);
    Assert.assertNotNull(rawScopes);
    Assert.assertEquals(rawScopes,
        flowOptions.getArguments().getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertTrue(provOptions.isDebug());

    RuntimeScopes scopes = RuntimeScopes.fromJson(rawScopes);
    // app.pipe1.env wins over cluster.mycluster.env and unscoped env
    Assert.assertEquals(Collections.singleton("s-fromApp"), scopes.getSecrets());
    Assert.assertFalse(scopes.hasExternalDynamicSecret());
    // Original user args remain unresolved in ProgramOptions (dispatcher resolves them at launch)
    Assert.assertEquals("unscoped", provOptions.getUserArguments().getOption("env"));

    // Cluster scope applies when app scope is absent; also verify explicit true fallback when unset
    Mockito.reset(provisionerNotifier);
    cConf.unset(Constants.RuntimeMonitor.RUNTIME_TOKEN_ENABLED);
    Map<String, String> clusterOnlyUserArgs = ImmutableMap.of(
        SystemArguments.PROFILE_NAME, SYSTEM_DATAPROC_PROFILE_ID.getScopedName(),
        "env", "unscoped",
        "cluster.mycluster.env", "fromCluster");
    lifecycleService.runInternal(WORKFLOW_ID, clusterOnlyUserArgs, sysArgs, false);
    RuntimeScopes clusterScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertEquals(Collections.singleton("s-fromCluster"), clusterScopes.getSecrets());
  }

  @Test
  public void testIssuanceDisabledAndInternalAuthOffStripAndOmitScopes() throws Exception {
    stubApp(appSpecWithProp("${secure(s1)}"));
    Map<String, String> userArgs = Collections.singletonMap(
        SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName());
    Map<String, String> prePopulatedSysArgs = Collections.singletonMap(
        ProgramOptionConstants.RUNTIME_SCOPES, RuntimeScopes.wildcardAll().toJson());

    // Issuance flag explicitly false -> pre-existing RUNTIME_SCOPES stripped and not re-added
    cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_ENABLED, false);
    lifecycleService.runInternal(WORKFLOW_ID, userArgs, prePopulatedSysArgs, false);
    Assert.assertNull(captureProvisioningOptions().getArguments()
        .getOption(ProgramOptionConstants.RUNTIME_SCOPES));

    // Re-enable issuance, turn off internal auth -> pre-existing RUNTIME_SCOPES stripped
    Mockito.reset(provisionerNotifier);
    cConf.setBoolean(Constants.RuntimeMonitor.RUNTIME_TOKEN_ENABLED, true);
    cConf.setBoolean(Constants.Security.INTERNAL_AUTH_ENABLED, false);
    lifecycleService.runInternal(WORKFLOW_ID, userArgs, prePopulatedSysArgs, false);
    Assert.assertNull(captureProvisioningOptions().getArguments()
        .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
  }

  @Test
  public void testNativeProfileTetheringAndSshMonitorTypeOmitScopes() throws Exception {
    stubApp(appSpecWithProp("${secure(s1)}"));
    Map<String, String> prePopulatedSysArgs = Collections.singletonMap(
        ProgramOptionConstants.RUNTIME_SCOPES, RuntimeScopes.wildcardAll().toJson());

    // Native profile -> no scopes (and strips any pre-existing RUNTIME_SCOPES)
    lifecycleService.runInternal(WORKFLOW_ID, Collections.emptyMap(),
        prePopulatedSysArgs, false);
    Assert.assertNull(captureProvisioningOptions().getArguments()
        .getOption(ProgramOptionConstants.RUNTIME_SCOPES));

    // Tethering provisioner (even with URL monitor type) -> no scopes
    Mockito.reset(provisionerNotifier);
    lifecycleService.runInternal(
        WORKFLOW_ID,
        Collections.singletonMap(SystemArguments.PROFILE_NAME,
            TETHERING_PROFILE_ID.getScopedName()),
        prePopulatedSysArgs, false);
    Assert.assertNull(captureProvisioningOptions().getArguments()
        .getOption(ProgramOptionConstants.RUNTIME_SCOPES));

    // Plain SSH user runtime argument cannot override URL-configured Dataproc profile;
    // scopes are still computed and stored (pre-populated wildcard is stripped and recomputed).
    Mockito.reset(provisionerNotifier);
    lifecycleService.runInternal(
        WORKFLOW_ID,
        ImmutableMap.of(
            SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName(),
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.SSH.name()),
        prePopulatedSysArgs, false);
    RuntimeScopes plainSshArgScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertEquals(Collections.singleton("s1"), plainSshArgScopes.getSecrets());
    Assert.assertFalse(plainSshArgScopes.hasExternalDynamicSecret());

    // Scoped workflow.<name>.system.runtime.monitor.type=ssh also cannot override URL config
    Mockito.reset(provisionerNotifier);
    lifecycleService.runInternal(
        WORKFLOW_ID,
        ImmutableMap.of(
            SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName(),
            "workflow." + WORKFLOW_ID.getProgram() + "." + SystemArguments.RUNTIME_MONITOR_TYPE,
            RuntimeMonitorType.SSH.name()),
        prePopulatedSysArgs, false);
    RuntimeScopes scopedSshArgScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertEquals(Collections.singleton("s1"), scopedSshArgScopes.getSecrets());
    Assert.assertFalse(scopedSshArgScopes.hasExternalDynamicSecret());

    // Operator-configured SSH monitor type in cConf (even with user arg URL) -> omits scopes
    Mockito.reset(provisionerNotifier);
    cConf.set(Constants.RuntimeMonitor.MONITOR_TYPE_PREFIX + "gcp-dataproc",
        RuntimeMonitorType.SSH.name());
    lifecycleService.runInternal(
        WORKFLOW_ID,
        ImmutableMap.of(
            SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName(),
            SystemArguments.RUNTIME_MONITOR_TYPE, RuntimeMonitorType.URL.name()),
        prePopulatedSysArgs, false);
    Assert.assertNull(captureProvisioningOptions().getArguments()
        .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
  }

  @Test
  public void testTriggerMappedArgumentsResolvedAndUnreadableWidens() throws Exception {
    cConf.setBoolean("feature." + Feature.PIPELINE_COMPOSITE_TRIGGERS.getFeatureFlagString(), true);
    stubApp(appSpecWithProp("${secure(${mappedSec})}"));
    Map<String, String> userArgs = Collections.singletonMap(
        SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName());

    // Valid trigger mapping supplies mappedSec -> exact secret extracted
    DefaultProgramStatusTriggerInfo triggerInfo = new DefaultProgramStatusTriggerInfo(
        NS.getNamespace(), "upstream", ProgramType.WORKFLOW, "DataPipelineWorkflow",
        RunIds.generate(), ProgramStatus.COMPLETED, new BasicWorkflowToken(1),
        Collections.singletonMap("srcArg", "sec-from-trigger"));
    TriggeringScheduleInfo scheduleInfo = new DefaultTriggeringScheduleInfo(
        "sched1", "desc",
        Collections.singletonList(triggerInfo),
        Collections.singletonMap("triggering.properties.mapping",
            "{\"arguments\":[{\"source\":\"srcArg\",\"target\":\"mappedSec\"}]}"));
    Map<String, String> validTriggerSysArgs = Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, SCHEDULE_GSON.toJson(scheduleInfo));

    lifecycleService.runInternal(WORKFLOW_ID, userArgs, validTriggerSysArgs, false);
    RuntimeScopes mappedScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertEquals(Collections.singleton("sec-from-trigger"), mappedScopes.getSecrets());
    Assert.assertFalse(mappedScopes.hasExternalDynamicSecret());

    // Unreadable trigger schedule JSON -> widens to wildcardAll() and launch proceeds
    Mockito.reset(provisionerNotifier);
    Map<String, String> malformedTriggerSysArgs = Collections.singletonMap(
        ProgramOptionConstants.TRIGGERING_SCHEDULE_INFO, "{not-valid-json");
    lifecycleService.runInternal(WORKFLOW_ID, userArgs, malformedTriggerSysArgs, false);
    RuntimeScopes widenedScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertEquals(RuntimeScopes.wildcardAll(), widenedScopes);
  }

  @Test
  public void testConnectionReadFailureAndExtractorExceptionWidenAndProceed() throws Exception {
    Map<String, String> userArgs = Collections.singletonMap(
        SystemArguments.PROFILE_NAME, DATAPROC_PROFILE_ID.getScopedName());

    // Real ConnectionPropertiesFetcher with unavailable studio service -> widens and proceeds
    stubApp(appSpecWithProp("${conn(my-conn)}"));
    ConnectionPropertiesFetcher realFetcher = new ConnectionPropertiesFetcher(
        new RemoteClientFactory(new InMemoryDiscoveryService(), setter -> { }));
    ProgramLifecycleService serviceWithRealFetcher = newLifecycleService(realFetcher);
    serviceWithRealFetcher.runInternal(WORKFLOW_ID, userArgs, Collections.emptyMap(), false);
    RuntimeScopes unavailableScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertTrue(unavailableScopes.getConnections().contains(RuntimeScopes.WILDCARD));
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        unavailableScopes.getOauth());
    Assert.assertTrue(unavailableScopes.hasExternalDynamicSecret());

    // Mocked connection read failure -> widens connections, oauth, dynamic secret; launch proceeds
    Mockito.reset(provisionerNotifier);
    Mockito.when(connectionPropertiesFetcher.forNamespace(NS.getNamespace()))
        .thenReturn(name -> {
          throw new IOException("studio unavailable");
        });
    lifecycleService.runInternal(WORKFLOW_ID, userArgs, Collections.emptyMap(), false);
    RuntimeScopes readFailScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertTrue(readFailScopes.getConnections().contains(RuntimeScopes.WILDCARD));
    Assert.assertEquals(Collections.singleton(RuntimeScopes.OAUTH_WILDCARD),
        readFailScopes.getOauth());
    Assert.assertTrue(readFailScopes.hasExternalDynamicSecret());

    // Unexpected exception inside extractor -> wildcardAll(); launch still proceeds
    Mockito.reset(provisionerNotifier);
    ApplicationSpecification validForCreate = appSpecWithProp("${secure(s1)}");
    ApplicationSpecification brokenAtExtract = Mockito.mock(ApplicationSpecification.class);
    Mockito.when(brokenAtExtract.getArtifactId()).thenReturn(SYSTEM_ARTIFACT);
    Mockito.when(brokenAtExtract.getPlugins()).thenReturn(Collections.emptyMap());
    Mockito.when(brokenAtExtract.getConfiguration()).thenThrow(new RuntimeException("boom"));
    ProgramDescriptor brokenDescriptor = new ProgramDescriptor(WORKFLOW_ID, brokenAtExtract);
    Mockito.when(store.getApplication(WORKFLOW_ID.getParent())).thenReturn(validForCreate);
    Mockito.when(store.loadProgram(WORKFLOW_ID)).thenReturn(brokenDescriptor);

    lifecycleService.runInternal(WORKFLOW_ID, userArgs, Collections.emptyMap(), false);
    RuntimeScopes errorScopes = RuntimeScopes.fromJson(
        captureProvisioningOptions().getArguments()
            .getOption(ProgramOptionConstants.RUNTIME_SCOPES));
    Assert.assertEquals(RuntimeScopes.wildcardAll(), errorScopes);
  }

  private void stubApp(ApplicationSpecification appSpec) throws Exception {
    Mockito.when(store.getApplication(WORKFLOW_ID.getParent())).thenReturn(appSpec);
    Mockito.when(store.loadProgram(WORKFLOW_ID))
        .thenReturn(new ProgramDescriptor(WORKFLOW_ID, appSpec));
  }

  private ProgramOptions captureFlowControlOptions() throws Exception {
    ArgumentCaptor<ProgramOptions> captor = ArgumentCaptor.forClass(ProgramOptions.class);
    Mockito.verify(flowControlService).addRequestAndGetCounter(
        Mockito.any(ProgramRunId.class), captor.capture(), Mockito.any(ProgramDescriptor.class));
    return captor.getValue();
  }

  private ProgramOptions captureProvisioningOptions() {
    ArgumentCaptor<ProgramOptions> captor = ArgumentCaptor.forClass(ProgramOptions.class);
    Mockito.verify(provisionerNotifier).provisioning(
        Mockito.any(ProgramRunId.class), captor.capture(),
        Mockito.any(ProgramDescriptor.class), Mockito.anyString());
    return captor.getValue();
  }

  private static ApplicationSpecification appSpecWithProp(String value) {
    Plugin plugin = new Plugin(
        Collections.emptyList(), SYSTEM_ARTIFACT,
        PluginClass.builder().setType("batchsource").setName("Src")
            .setDescription("desc").setClassName("io.cdap.Src").build(),
        PluginProperties.builder().add("prop", value).build());
    WorkflowSpecification wfSpec = new WorkflowSpecification(
        "io.cdap.Wf", WORKFLOW_ID.getProgram(), "desc",
        Collections.emptyMap(), Collections.emptyList(),
        Collections.emptyMap(), Collections.emptyMap());
    return new DefaultApplicationSpecification(
        WORKFLOW_ID.getApplication(), WORKFLOW_ID.getVersion(), null, "desc", null,
        SYSTEM_ARTIFACT,
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap(),
        Collections.singletonMap(WORKFLOW_ID.getProgram(), wfSpec),
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(),
        new HashMap<>(Collections.singletonMap("src", plugin)));
  }
}
