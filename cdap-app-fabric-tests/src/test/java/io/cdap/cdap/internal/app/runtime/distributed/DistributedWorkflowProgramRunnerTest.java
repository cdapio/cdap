/*
 * Copyright © 2018-2022 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.Resources;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.app.DefaultAppConfigurer;
import io.cdap.cdap.app.DefaultApplicationContext;
import io.cdap.cdap.app.guice.AppFabricServiceRuntimeModule;
import io.cdap.cdap.app.guice.AuthorizationModule;
import io.cdap.cdap.app.guice.ProgramRunnerRuntimeModule;
import io.cdap.cdap.app.guice.TwillModule;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.app.program.Programs;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.KafkaClientModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.guice.RemoteAuthenticatorModules;
import io.cdap.cdap.common.guice.ZKClientModule;
import io.cdap.cdap.common.guice.ZKDiscoveryModule;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.data.runtime.DataFabricModules;
import io.cdap.cdap.data.runtime.DataSetServiceModules;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data2.audit.AuditModule;
import io.cdap.cdap.data2.metadata.writer.DefaultMetadataServiceClient;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.explore.guice.ExploreClientModule;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.logging.guice.LocalLogAppenderModule;
import io.cdap.cdap.messaging.guice.MessagingClientModule;
import io.cdap.cdap.metrics.guice.MetricsClientRuntimeModule;
import io.cdap.cdap.metrics.guice.MetricsStoreModule;
import io.cdap.cdap.operations.guice.OperationalStatsModule;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.authorization.AuthorizationEnforcementModule;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.cdap.security.guice.SecureStoreServerModule;
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
      RemoteAuthenticatorModules.getNoOpModule(),
      new ZKClientModule(),
      new ZKDiscoveryModule(),
      new LocalLogAppenderModule(),
      new LocalLocationModule(),
      new IOModule(),
      new KafkaClientModule(),
      new DataSetServiceModules().getDistributedModules(),
      new DataFabricModules("cdap.master").getDistributedModules(),
      new DataSetsModules().getDistributedModules(),
      new MetricsClientRuntimeModule().getDistributedModules(),
      new MetricsStoreModule(),
      new MessagingClientModule(),
      new ExploreClientModule(),
      new AuditModule(),
      CoreSecurityRuntimeModule.getDistributedModule(cConf),
      new AuthenticationContextModules().getNoOpModule(),
      new AuthorizationModule(),
      new AuthorizationEnforcementModule().getMasterModule(),
      new TwillModule(),
      new AppFabricServiceRuntimeModule(cConf).getDistributedModules(),
      new ProgramRunnerRuntimeModule().getDistributedModules(),
      new SecureStoreServerModule(),
      new OperationalStatsModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          // TODO (CDAP-14677): find a better way to inject metadata publisher
          bind(MetadataServiceClient.class).to(DefaultMetadataServiceClient.class);
        }
      });

    return injector.getInstance(ProgramRunnerFactory.class);
  }

  private static CConfiguration createCConf() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    return cConf;
  }
}
