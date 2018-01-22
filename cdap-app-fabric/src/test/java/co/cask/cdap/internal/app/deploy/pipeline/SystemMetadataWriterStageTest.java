/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.WorkflowAppWithFork;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.pipeline.StageContext;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.api.metadata.MetadataScope;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Injector;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
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
  private static MetadataStore metadataStore;

  @BeforeClass
  public static void setup() {
    Injector injector = AppFabricTestHelper.getInjector();
    metadataStore = injector.getInstance(MetadataStore.class);
  }

  @Test
  public void testWorkflowTags() throws Exception {
    String appName = WorkflowAppWithFork.class.getSimpleName();
    ApplicationId appId = NamespaceId.DEFAULT.app(appName);
    String workflowName = WorkflowAppWithFork.WorkflowWithFork.class.getSimpleName();
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact(appId.getApplication(), "1.0");
    ApplicationWithPrograms appWithPrograms = createAppWithWorkflow(artifactId, appId, workflowName);
    WorkflowSpecification workflowSpec = appWithPrograms.getSpecification().getWorkflows().get(workflowName);
    SystemMetadataWriterStage systemMetadataWriterStage = new SystemMetadataWriterStage(metadataStore);
    StageContext stageContext = new StageContext(Object.class);
    systemMetadataWriterStage.process(stageContext);
    systemMetadataWriterStage.process(appWithPrograms);

    // verify that the workflow is not tagged with the fork node name
    Set<String> workflowSystemTags = metadataStore.getTags(MetadataScope.SYSTEM, appId.workflow(workflowName));
    Sets.SetView<String> intersection = Sets.intersection(workflowSystemTags, getWorkflowForkNodes(workflowSpec));
    Assert.assertTrue("Workflows should not be tagged with fork node names, but found the following fork nodes " +
                        "in the workflow's system tags: " + intersection, intersection.isEmpty());

    // verify that metadata was added for the workflow's schedule
    Map<String, String> metadataProperties = metadataStore.getMetadata(MetadataScope.SYSTEM, appId).getProperties();
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
