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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.operation.LongRunningOperationContext;
import io.cdap.cdap.internal.operation.OperationException;
import io.cdap.cdap.proto.ApplicationDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.CommitMeta;
import io.cdap.cdap.sourcecontrol.RepositoryManager;
import io.cdap.cdap.sourcecontrol.RepositoryManagerFactory;
import io.cdap.cdap.sourcecontrol.operationrunner.InMemorySourceControlOperationRunner;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class PushAppsOperationTest {

  private static final ApplicationDetail testApp1Details = new ApplicationDetail(
      "testApp", "v1", "description1", null, null, "conf1", new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(), null, null);
  private static final ApplicationDetail testApp2Details = new ApplicationDetail(
      "testApp2", "v2", "description2", null, null, "conf2", new ArrayList<>(),
      new ArrayList<>(), new ArrayList<>(), null, null);

  private static final Gson GSON = new Gson();

  private final PushAppsRequest req = new PushAppsRequest(
      ImmutableSet.of(testApp1Details.getName(), testApp2Details.getName()),
      new RepositoryConfig.Builder()
          .setProvider(Provider.GITHUB)
          .setLink("test")
          .setDefaultBranch("")
          .setAuth(new AuthConfig(AuthType.PAT, new PatConfig("test", "test")))
          .build(),
      new CommitMeta("", "", 0, "test_commit")
  );

  private InMemorySourceControlOperationRunner opRunner;
  private LongRunningOperationContext context;

  private final Path path1 = Paths.get(testApp1Details.getName() + ".json");
  private final Path path2 = Paths.get(testApp2Details.getName() + ".json");

  private static final RepositoryManager mockRepositoryManager = Mockito.mock(
      RepositoryManager.class);


  @ClassRule
  public static TemporaryFolder repositoryBase = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    RepositoryManagerFactory mockRepositoryManagerFactory = Mockito.mock(
        RepositoryManagerFactory.class);
    Mockito.doReturn(mockRepositoryManager).when(mockRepositoryManagerFactory)
        .create(Mockito.any(), Mockito.any());
    Mockito.doNothing().when(mockRepositoryManager).close();

    Path rootPath = repositoryBase.getRoot().toPath();
    Mockito.doReturn(rootPath).when(mockRepositoryManager).getRepositoryRoot();
    Mockito.doReturn(rootPath).when(mockRepositoryManager).getBasePath();
    Mockito.doReturn(path1.getFileName()).when(mockRepositoryManager)
        .getFileRelativePath(path1.getFileName().toString());
    Mockito.doReturn(path2.getFileName()).when(mockRepositoryManager)
        .getFileRelativePath(path2.getFileName().toString());

    this.context = Mockito.mock(LongRunningOperationContext.class);
    Mockito.doReturn(new OperationRunId(NamespaceId.DEFAULT.getNamespace(), "id"))
        .when(this.context).getRunId();

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
    PushAppsOperation operation = new PushAppsOperation(req, opRunner, mockManager);

    Mockito.doReturn(testApp1Details).when(mockManager).get(
        new ApplicationReference(NamespaceId.DEFAULT.getNamespace(), testApp1Details.getName()));

    Mockito.doReturn(testApp2Details).when(mockManager).get(
        new ApplicationReference(NamespaceId.DEFAULT.getNamespace(), testApp2Details.getName()));

    PushAppResponse mockTestApp1Response = new PushAppResponse(
        testApp1Details.getName(), testApp1Details.getAppVersion(), "file-hash1");
    PushAppResponse mockTestApp2Response = new PushAppResponse(
        testApp2Details.getName(), testApp2Details.getAppVersion(), "file-hash2");

    Mockito.doReturn(ImmutableList.of(mockTestApp1Response, mockTestApp2Response))
        .when(mockRepositoryManager)
        .commitAndPush(Mockito.any(CommitMeta.class), Mockito.anyCollection(), Mockito.any(BiFunction.class));

    Set<OperationResource> gotResources = operation.run(context).get();

    verifyCreatedResources(gotResources);
  }

  @Test(expected = OperationException.class)
  public void testRunFailedAtFirstApp() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    LongRunningOperationContext mockContext = Mockito.mock(LongRunningOperationContext.class);
    PushAppsOperation operation = new PushAppsOperation(req, opRunner, mockManager);

    Mockito.doThrow(new NotFoundException("")).when(mockManager)
        .get(Mockito.any());

    operation.run(context).get();
  }

  @Test(expected = OperationException.class)
  public void testRunFailedAtSecondApp() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    LongRunningOperationContext mockContext = Mockito.mock(LongRunningOperationContext.class);
    PushAppsOperation operation = new PushAppsOperation(req, opRunner, mockManager);

    Mockito.doReturn(testApp1Details).doThrow(new NotFoundException("")).when(mockManager)
        .get(Mockito.any());

    operation.run(context).get();
  }

  @Test
  public void testRunFailedWhenMarkingLatest() throws Exception {
    ApplicationManager mockManager = Mockito.mock(ApplicationManager.class);
    PushAppsOperation operation = new PushAppsOperation(req, opRunner, mockManager);

    Mockito.doReturn(testApp1Details).when(mockManager).get(
        new ApplicationReference(NamespaceId.DEFAULT.getNamespace(), testApp1Details.getName()));

    Mockito.doReturn(testApp2Details).when(mockManager).get(
        new ApplicationReference(NamespaceId.DEFAULT.getNamespace(), testApp2Details.getName()));

    PushAppResponse mockTestApp1Response = new PushAppResponse(
        testApp1Details.getName(), testApp1Details.getAppVersion(), "file-hash1");
    PushAppResponse mockTestApp2Response = new PushAppResponse(
        testApp2Details.getName(), testApp2Details.getAppVersion(), "file-hash2");

    Mockito.doReturn(ImmutableList.of(mockTestApp1Response, mockTestApp2Response))
        .when(mockRepositoryManager)
        .commitAndPush(Mockito.any(CommitMeta.class), Mockito.anyCollection(), Mockito.any(BiFunction.class));

    Mockito.doThrow(new NotFoundException("")).when(mockManager)
        .updateSourceControlMeta(Mockito.any(), Mockito.any());

    Set<OperationResource> gotResources = new HashSet<>();
    Mockito.doAnswer(i -> {
      gotResources.addAll(
          (Collection<? extends OperationResource>) i.getArguments()[0]);
      return null;
    }).when(context).updateOperationResources(Mockito.any());
    try {
      operation.run(context).get();
      Assert.fail();
    } catch (OperationException e) {
      // expected
    }

    verifyCreatedResources(gotResources);
  }

  private void verifyCreatedResources(Set<OperationResource> gotResources) {
    Set<ApplicationId> expectedAppIds = ImmutableSet.of(
        new ApplicationId(NamespaceId.DEFAULT.getNamespace(), testApp1Details.getName(),
            testApp1Details.getAppVersion()),
        new ApplicationId(NamespaceId.DEFAULT.getNamespace(), testApp2Details.getName(),
            testApp2Details.getAppVersion())
    );
    Set<ApplicationId> createdAppIds = gotResources.stream()
        .map(r -> ApplicationId.fromString(r.getResourceUri())).collect(Collectors.toSet());
    Assert.assertEquals(expectedAppIds, createdAppIds);
  }

}
