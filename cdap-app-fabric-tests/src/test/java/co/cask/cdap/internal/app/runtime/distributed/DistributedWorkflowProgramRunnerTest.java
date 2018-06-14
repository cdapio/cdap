/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.guice.AppFabricServiceRuntimeModule;
import co.cask.cdap.app.guice.AuthorizationModule;
import co.cask.cdap.app.guice.ProgramRunnerRuntimeModule;
import co.cask.cdap.app.guice.ServiceStoreModules;
import co.cask.cdap.app.guice.TwillModule;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.DiscoveryRuntimeModule;
import co.cask.cdap.common.guice.IOModule;
import co.cask.cdap.common.guice.KafkaClientModule;
import co.cask.cdap.common.guice.LocationRuntimeModule;
import co.cask.cdap.common.guice.ZKClientModule;
import co.cask.cdap.common.id.Id;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.data.runtime.DataFabricModules;
import co.cask.cdap.data.runtime.DataSetServiceModules;
import co.cask.cdap.data.runtime.DataSetsModules;
import co.cask.cdap.data.stream.StreamAdminModules;
import co.cask.cdap.data.view.ViewAdminModules;
import co.cask.cdap.data2.audit.AuditModule;
import co.cask.cdap.explore.guice.ExploreClientModule;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.logging.guice.LoggingModules;
import co.cask.cdap.messaging.guice.MessagingClientModule;
import co.cask.cdap.metrics.guice.MetricsClientRuntimeModule;
import co.cask.cdap.metrics.guice.MetricsStoreModule;
import co.cask.cdap.notifications.feeds.guice.NotificationFeedServiceRuntimeModule;
import co.cask.cdap.notifications.guice.NotificationServiceRuntimeModule;
import co.cask.cdap.operations.guice.OperationalStatsModule;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.guice.SecureStoreModules;
import co.cask.cdap.store.guice.NamespaceStoreModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.Configs;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Unit tests for the {@link DistributedWorkflowProgramRunner}.
 * This test class uses {@link DistributedWorkflowTestApp} for testing various aspect of the program runner.
 */
public class DistributedWorkflowProgramRunnerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static CConfiguration cConf;
  private static ProgramRunnerFactory programRunnerFactory;

  @BeforeClass
  public static void init() throws IOException {
    cConf = createCConf();
    programRunnerFactory = createProgramRunnerFactory(cConf);
  }

  @Test
  public void testDefaultResources() throws IOException {
    // By default the workflow driver would have 768m if none of the children is setting it to higher
    // (default for programs are 512m)
    testDriverResources(DistributedWorkflowTestApp.SequentialWorkflow.class.getSimpleName(),
                        Collections.emptyMap(), new Resources(768));
  }

  @Test
  public void testExplicitResources() throws IOException {
    // Explicitly set the resources for the workflow, it should always get honored.
    // The one prefixed with "task.workflow." should override the one without.
    testDriverResources(DistributedWorkflowTestApp.ComplexWorkflow.class.getSimpleName(),
                        ImmutableMap.of(
                          SystemArguments.MEMORY_KEY, "4096",
                          "task.workflow." + SystemArguments.MEMORY_KEY, "1024"
                        ),
                        new Resources(1024));
  }

  @Test
  public void testInferredResources() throws IOException {
    // Inferred from the largest memory usage from children
    testDriverResources(DistributedWorkflowTestApp.SequentialWorkflow.class.getSimpleName(),
                        Collections.singletonMap("mapreduce.mr1." + SystemArguments.MEMORY_KEY, "2048"),
                        new Resources(2048));
  }

  @Test
  public void testForkJoinInferredResources() throws IOException {
    // The complex workflow has 4 parallel executions at max, hence the memory setting should be summation of them
    // For vcores, it should pick the max
    testDriverResources(DistributedWorkflowTestApp.ComplexWorkflow.class.getSimpleName(),
                        ImmutableMap.of(
                          "spark.s2." + SystemArguments.MEMORY_KEY, "1024",
                          "mapreduce.mr3." + SystemArguments.CORES_KEY, "3"
                        ),
                        new Resources(512 + 512 + 1024 + 512, 3));
  }

  @Test
  public void testConditionInferredResources() throws IOException {
    // Set one of the branch to have memory > fork memory
    testDriverResources(DistributedWorkflowTestApp.ComplexWorkflow.class.getSimpleName(),
                        Collections.singletonMap("spark.s4." + SystemArguments.MEMORY_KEY, "4096"),
                        new Resources(4096));
  }

  @Test
  public void testInferredWithMaxResources() throws IOException {
    // Set to use large memory for the first node in the complex workflow to have it larger than the sum of all forks
    testDriverResources(DistributedWorkflowTestApp.ComplexWorkflow.class.getSimpleName(),
                        Collections.singletonMap("mapreduce.mr1." + SystemArguments.MEMORY_KEY, "4096"),
                        new Resources(4096));
  }

  @Test
  public void testInferredScopedResources() throws IOException {
    // Inferred from the largest memory usage from children.
    // Make sure the children arguments have scope resolved correctly.
    testDriverResources(DistributedWorkflowTestApp.SequentialWorkflow.class.getSimpleName(),
                        ImmutableMap.of(
                          "mapreduce.mr1.task.mapper." + SystemArguments.MEMORY_KEY, "2048",
                          "spark.s1.task.client." + SystemArguments.MEMORY_KEY, "1024",
                          "spark.s1.task.driver." + SystemArguments.MEMORY_KEY, "4096"
                        ),
                        // Should pick the spark client memory
                        new Resources(1024));
  }

  @Test
  public void testOverrideInferredResources() throws IOException {
    // Explicitly setting memory always override what's inferred from children
    testDriverResources(DistributedWorkflowTestApp.SequentialWorkflow.class.getSimpleName(),
                        ImmutableMap.of(
                          "task.workflow." + SystemArguments.MEMORY_KEY, "512",
                          "mapreduce.mr1." + SystemArguments.MEMORY_KEY, "2048",
                          "spark.s1." + SystemArguments.MEMORY_KEY, "1024"
                        ), new Resources(512));
  }

  @Test
  public void testReservedMemoryOverride() throws IOException {
    // Sets the reserved memory override for the workflow
    String workflowName = DistributedWorkflowTestApp.SequentialWorkflow.class.getSimpleName();
    ProgramLaunchConfig launchConfig = setupWorkflowRuntime(workflowName,
                                                            ImmutableMap.of(
                                                              SystemArguments.RESERVED_MEMORY_KEY_OVERRIDE, "400",
                                                              SystemArguments.MEMORY_KEY, "800"
                                                            ));

    RunnableDefinition runnableDefinition = launchConfig.getRunnables().get(workflowName);
    Assert.assertNotNull(runnableDefinition);

    Map<String, String> twillConfigs = runnableDefinition.getTwillRunnableConfigs();
    Assert.assertEquals("400", twillConfigs.get(Configs.Keys.JAVA_RESERVED_MEMORY_MB));
    Assert.assertEquals("0.50", twillConfigs.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO));
  }


  /**
   * Helper method to help testing workflow driver resources settings based on varying user arguments
   *
   * @param workflowName name of the workflow as defined in the {@link DistributedWorkflowTestApp}.
   * @param runtimeArgs the runtime arguments for the workflow program
   * @param expectedDriverResources the expected {@link Resources} setting for the workflow driver
   */
  private void testDriverResources(String workflowName, Map<String, String> runtimeArgs,
                                   Resources expectedDriverResources) throws IOException {

    ProgramLaunchConfig launchConfig = setupWorkflowRuntime(workflowName, runtimeArgs);

    // Validate the resources setting
    RunnableDefinition runnableDefinition = launchConfig.getRunnables().get(workflowName);
    Assert.assertNotNull(runnableDefinition);

    ResourceSpecification resources = runnableDefinition.getResources();
    Assert.assertEquals(expectedDriverResources.getMemoryMB(), resources.getMemorySize());
    Assert.assertEquals(expectedDriverResources.getVirtualCores(), resources.getVirtualCores());
  }

  /**
   * Setup the {@link ProgramLaunchConfig} for the given workflow.
   */
  private ProgramLaunchConfig setupWorkflowRuntime(String workflowName,
                                                   Map<String, String> runtimeArgs) throws IOException {
    // Create the distributed workflow program runner
    ProgramRunner programRunner = programRunnerFactory.create(ProgramType.WORKFLOW);
    Assert.assertTrue(programRunner instanceof DistributedWorkflowProgramRunner);
    DistributedWorkflowProgramRunner workflowRunner = (DistributedWorkflowProgramRunner) programRunner;

    // Create the Workflow Program
    Program workflowProgram = createWorkflowProgram(cConf, programRunner, workflowName);
    ProgramLaunchConfig launchConfig = new ProgramLaunchConfig();
    ProgramOptions programOpts = new SimpleProgramOptions(workflowProgram.getId(),
                                                          new BasicArguments(), new BasicArguments(runtimeArgs));

    // Setup the launching config
    workflowRunner.setupLaunchConfig(launchConfig, workflowProgram, programOpts,
                                     cConf, new Configuration(), TEMP_FOLDER.newFolder());
    return launchConfig;
  }

  /**
   * Creates a workflow {@link Program}.
   */
  private Program createWorkflowProgram(CConfiguration cConf, ProgramRunner programRunner,
                                        String workflowName) throws IOException {
    Location appJarLocation = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TEMP_FOLDER.newFolder()),
                                                               DistributedWorkflowTestApp.class);
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("test", "1.0.0");
    DistributedWorkflowTestApp app = new DistributedWorkflowTestApp();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(Id.Namespace.DEFAULT,
                                                               Id.Artifact.fromEntityId(artifactId), app);
    app.configure(configurer, new DefaultApplicationContext<>());

    ApplicationSpecification appSpec = configurer.createSpecification(null);
    ProgramId programId = NamespaceId.DEFAULT
      .app(appSpec.getName())
      .program(ProgramType.WORKFLOW, workflowName);

    return Programs.create(cConf, programRunner, new ProgramDescriptor(programId, appSpec),
                           appJarLocation, TEMP_FOLDER.newFolder());
  }

  private static ProgramRunnerFactory createProgramRunnerFactory(CConfiguration cConf) {
    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new ZKClientModule(),
      new LoggingModules().getDistributedModules(),
      new LocationRuntimeModule().getStandaloneModules(),
      new IOModule(),
      new KafkaClientModule(),
      new DiscoveryRuntimeModule().getDistributedModules(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules("cdap.master").getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new ExploreClientModule(),
      new NotificationFeedServiceRuntimeModule().getDistributedModules(),
      new NotificationServiceRuntimeModule().getDistributedModules(),
      new ViewAdminModules().getDistributedModules(),
      new StreamAdminModules().getDistributedModules(),
      new NamespaceStoreModule().getDistributedModules(),
      new AuditModule().getDistributedModules(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new TwillModule(),
      new ServiceStoreModules().getDistributedModules(),
      new AppFabricServiceRuntimeModule().getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SecureStoreModules().getDistributedModules(),
      new OperationalStatsModule()
    );

    return injector.getInstance(ProgramRunnerFactory.class);
  }

  private static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    return cConf;
  }
}
