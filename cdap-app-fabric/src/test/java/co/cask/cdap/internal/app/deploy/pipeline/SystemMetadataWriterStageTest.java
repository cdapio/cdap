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
import co.cask.cdap.app.DefaultAppConfigurer;
import co.cask.cdap.app.DefaultApplicationContext;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.archive.ArchiveBundler;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.app.program.ProgramBundle;
import co.cask.cdap.internal.app.runtime.artifact.ArtifactRepository;
import co.cask.cdap.internal.pipeline.StageContext;
import co.cask.cdap.internal.test.AppJarHelper;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.metadata.MetadataScope;
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
  private static ArtifactRepository artifactRepository;

  @BeforeClass
  public static void setup() {
    Injector injector = AppFabricTestHelper.getInjector();
    metadataStore = injector.getInstance(MetadataStore.class);
    artifactRepository = injector.getInstance(ArtifactRepository.class);
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
    Set<String> workflowSystemTags = metadataStore.getTags(MetadataScope.SYSTEM, appId.workflow(workflowName).toId());
    Sets.SetView<String> intersection = Sets.intersection(workflowSystemTags, getWorkflowForkNodes(workflowSpec));
    Assert.assertTrue("Workflows should not be tagged with fork node names, but found the following fork nodes " +
                        "in the workflow's system tags: " + intersection, intersection.isEmpty());
  }

  @SuppressWarnings("unchecked")
  private ApplicationWithPrograms createAppWithWorkflow(ArtifactId artifactId, ApplicationId appId,
                                                        String workflowName) throws IOException {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    AbstractApplication app = new WorkflowAppWithFork();
    DefaultAppConfigurer configurer = new DefaultAppConfigurer(NamespaceId.DEFAULT.toId(), artifactId.toId(), app, "",
                                                               artifactRepository, null);
    app.configure(configurer, new DefaultApplicationContext());
    ApplicationSpecification appSpec = configurer.createSpecification();

    Location workflowJar = AppJarHelper.createDeploymentJar(locationFactory, WorkflowAppWithFork.class);
    ArchiveBundler bundler = new ArchiveBundler(workflowJar);
    Location programLocation = ProgramBundle.create(appId.workflow(workflowName).toId(), bundler,
                                                    locationFactory.create("programs"), workflowName, appSpec);
    Program workflow = Programs.create(programLocation);
    ApplicationDeployable appDeployable = new ApplicationDeployable(
      appId.toId(), appSpec, null, ApplicationDeployScope.USER, locationFactory.create("apps"));
    return new ApplicationWithPrograms(appDeployable, ImmutableList.of(workflow));
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
