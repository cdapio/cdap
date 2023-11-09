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


import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.operation.LongRunningOperationContext;
import io.cdap.cdap.internal.operation.OperationException;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.SourceControlException;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class PullAppsOperationTest {

  private static final AppRequest<?> TEST_APP_REQUEST = new AppRequest<>(
      new ArtifactSummary("name", "version"));

  private static final Gson GSON = new Gson();

  private final PullAppsRequest req = new PullAppsRequest(ImmutableSet.of("1", "2", "3", "4"),
      null);

  private InMemorySourceControlOperationRunner opRunner;
  private LongRunningOperationContext context;
  private Set<OperationResource> gotResources;

  @ClassRule
  public static TemporaryFolder repositoryBase = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    RepositoryManagerFactory mockRepositoryManagerFactory = Mockito.mock(
        RepositoryManagerFactory.class);
    RepositoryManager mockRepositoryManager = Mockito.mock(RepositoryManager.class);
    Mockito.doReturn(mockRepositoryManager).when(mockRepositoryManagerFactory)
        .create(Mockito.any(), Mockito.any());

    Path rootPath = repositoryBase.getRoot().toPath();
    Mockito.doReturn(rootPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(rootPath).when(mockRepositoryManager).getBasePath();

    Mockito.doReturn("testHash")
        .when(mockRepositoryManager)
        .getFileHash(Mockito.any(), Mockito.any());
    Path tempFile = repositoryBase.newFile().toPath();
    Files.write(tempFile, GSON.toJson(TEST_APP_REQUEST).getBytes(StandardCharsets.UTF_8));
    Mockito.doNothing().when(mockRepositoryManager).close();
    Mockito.doReturn(tempFile.getFileName()).when(mockRepositoryManager)
        .getFileRelativePath(Mockito.any());

    this.context = Mockito.mock(LongRunningOperationContext.class);
    Mockito.doReturn(new OperationRunId("default", "id")).when(this.context).getRunId();
    gotResources = new HashSet<>();
    Mockito.doAnswer(i -> {
      gotResources.addAll(
          (Collection<? extends OperationResource>) i.getArguments()[0]);
      return null;
    }).when(context).updateOperationResources(Mockito.any());

    Injector injector = AppFabricTestHelper.getInjector(CConfiguration.create(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(RepositoryManagerFactory.class).toInstance(mockRepositoryManagerFactory);
          }
        });

    this.opRunner = injector.getInstance(InMemorySourceControlOperationRunner.class);
  }

  @Test
  public void testRunSuccess() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    PullAppsOperation operation = new PullAppsOperation(this.req, opRunner, mockManager);

    Mockito.doAnswer(i -> {
          ApplicationReference appref = (ApplicationReference) i.getArguments()[0];
          return appref.app(appref.getApplication());
        }
    ).when(mockManager).deployApp(Mockito.any(), Mockito.any());

    gotResources = operation.run(context).get();

    verifyCreatedResources(ImmutableSet.of("1", "2", "3", "4"));
  }

  @Test(expected = OperationException.class)
  public void testRunFailedAtFirstApp() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    LongRunningOperationContext mockContext = Mockito.mock(LongRunningOperationContext.class);
    PullAppsOperation operation = new PullAppsOperation(this.req, opRunner, mockManager);

    Mockito.doThrow(new SourceControlException("")).when(mockManager)
        .deployApp(Mockito.any(), Mockito.any());

    operation.run(context).get();
  }

  @Test
  public void testRunFailedAtSecondApp() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    PullAppsOperation operation = new PullAppsOperation(this.req, opRunner, mockManager);

    Mockito.doAnswer(
            i -> {
              ApplicationReference appref = (ApplicationReference) i.getArguments()[0];
              return appref.app(appref.getApplication());
            }
        ).doThrow(new SourceControlException("")).when(mockManager)
        .deployApp(Mockito.any(), Mockito.any());

    try {
      operation.run(context).get();
    } catch (Exception e) {
      // expected
    }

    verifyCreatedResources(ImmutableSet.of("1"));
  }

  @Test
  public void testRunFailedWhenMarkingLatest() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    PullAppsOperation operation = new PullAppsOperation(this.req, opRunner, mockManager);

    Mockito.doAnswer(
        i -> {
          ApplicationReference appref = (ApplicationReference) i.getArguments()[0];
          return appref.app(appref.getApplication());
        }
    ).when(mockManager).deployApp(Mockito.any(), Mockito.any());

    Mockito.doThrow(new SourceControlException("")).when(mockManager)
        .markAppVersionsLatest(Mockito.any(), Mockito.any());

    try {
      operation.run(context).get();
    } catch (Exception e) {
      // expected
    }

    verifyCreatedResources(ImmutableSet.of("1", "2", "3", "4"));
  }

  private void verifyCreatedResources(Set<String> expectedApps) {
    Set<ApplicationId> createdAppIds = gotResources.stream()
        .map(r -> ApplicationId.fromString(r.getResourceUri())).collect(Collectors.toSet());
    Set<ApplicationId> expectedAppIds = expectedApps.stream()
        .map(a -> new ApplicationId("default", a, a)).collect(Collectors.toSet());
    Assert.assertEquals(expectedAppIds, createdAppIds);
  }

}
