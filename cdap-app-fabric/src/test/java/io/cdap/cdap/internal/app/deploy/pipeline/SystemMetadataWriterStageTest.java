/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import io.cdap.cdap.WorkflowAppWithFork;
import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowNodeType;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.deploy.Specifications;
import io.cdap.cdap.internal.pipeline.StageContext;
import io.cdap.cdap.metadata.MetadataSubscriberService;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataStorage;
import io.cdap.cdap.spi.metadata.Read;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link SystemMetadataWriterStage}.
 */
public class SystemMetadataWriterStageTest {
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static MetadataStorage metadataStorage;
  private static MetadataServiceClient metadataServiceClient;
  private static MetadataSubscriberService metadataSubscriber;

  @BeforeClass
  public static void setup() {
    Injector injector = AppFabricTestHelper.getInjector();
    metadataStorage = injector.getInstance(MetadataStorage.class);
    metadataServiceClient = injector.getInstance(MetadataServiceClient.class);
    metadataSubscriber = injector.getInstance(MetadataSubscriberService.class);
    metadataSubscriber.startAndWait();
  }

  @AfterClass
  public static void stop() {
    metadataSubscriber.stopAndWait();
    AppFabricTestHelper.shutdown();
  }

  @Test
  public void testWorkflowTags() throws Exception {
    String appName = WorkflowAppWithFork.class.getSimpleName();
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    String workflowName = WorkflowAppWithFork.WorkflowWithFork.class.getSimpleName();
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact(appId.getApplication(), "1.0");
    ApplicationWithPrograms appWithPrograms = createAppWithWorkflow(artifactId, appId, workflowName);
    WorkflowSpecification workflowSpec = appWithPrograms.getSpecification().getWorkflows().get(workflowName);
    SystemMetadataWriterStage systemMetadataWriterStage = new SystemMetadataWriterStage(metadataServiceClient);
    StageContext stageContext = new StageContext(Object.class);
    systemMetadataWriterStage.process(stageContext);
    systemMetadataWriterStage.process(appWithPrograms);

    Assert.assertEquals(false, metadataStorage.read(new Read(appId.workflow(workflowName).toMetadataEntity(),
                                                             MetadataScope.SYSTEM, MetadataKind.TAG)).isEmpty());
    Set<String> workflowSystemTags = metadataStorage
      .read(new Read(appId.workflow(workflowName).toMetadataEntity())).getTags(MetadataScope.SYSTEM);
    Sets.SetView<String> intersection = Sets.intersection(workflowSystemTags, getWorkflowForkNodes(workflowSpec));
    Assert.assertTrue("Workflows should not be tagged with fork node names, but found the following fork nodes " +
                        "in the workflow's system tags: " + intersection, intersection.isEmpty());

    Assert.assertEquals(false, metadataStorage.read(new Read(appId.toMetadataEntity(),
                                                             MetadataScope.SYSTEM, MetadataKind.PROPERTY)).isEmpty());
    Map<String, String> metadataProperties = metadataStorage
      .read(new Read(appId.toMetadataEntity())).getProperties(MetadataScope.SYSTEM);
    Assert.assertEquals(WorkflowAppWithFork.SCHED_NAME + ":testDescription",
                        metadataProperties.get("schedule:" + WorkflowAppWithFork.SCHED_NAME));
  }

  @SuppressWarnings("unchecked")
  private ApplicationWithPrograms createAppWithWorkflow(ArtifactId artifactId, ApplicationId appId,
                                                        String workflowName) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    AbstractApplication app = new WorkflowAppWithFork();
    ApplicationSpecification appSpec = Specifications.from(app);
    Location workflowJar = AppJarHelper.createDeploymentJar(locationFactory, WorkflowAppWithFork.class);
    ApplicationDeployable appDeployable = new ApplicationDeployable(artifactId, workflowJar,
                                                                    appId, appSpec, null, ApplicationDeployScope.USER);
    return new ApplicationWithPrograms(appDeployable,
                                       ImmutableList.of(new ProgramDescriptor(appId.workflow(workflowName), appSpec)));
  }

  private Set<String> getWorkflowForkNodes(WorkflowSpecification workflowSpec) {
    ImmutableSet.Builder<String> nodes = new ImmutableSet.Builder<>();
    for (Map.Entry<String, WorkflowNode> entry : workflowSpec.getNodeIdMap().entrySet()) {
      if (WorkflowNodeType.FORK == entry.getValue().getType()) {
        nodes.add(entry.getKey());
      }
    }
    return nodes.build();
  }
}
