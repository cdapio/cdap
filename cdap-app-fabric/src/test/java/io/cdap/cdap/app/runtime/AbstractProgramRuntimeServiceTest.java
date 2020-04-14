/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.program.ProgramDescriptor;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramControllerServiceAdapter;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDescriptor;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactDetail;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactMeta;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.ProgramLiveInfo;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * Unit-test for AbstractProgramRuntimeService base functionalities.
 */
public class AbstractProgramRuntimeServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test (timeout = 5000)
  public void testDeadlock() throws IOException, ExecutionException, InterruptedException, TimeoutException {
    // This test is for testing condition in (CDAP-3579)
    // The race condition is if a program finished very fast such that inside the AbstractProgramRuntimeService is
    // still in the run method, it holds the object lock, making the callback from the listener block forever.
    ProgramRunnerFactory runnerFactory = createProgramRunnerFactory();
    final Program program = createDummyProgram();
    final ProgramRuntimeService runtimeService =
      new AbstractProgramRuntimeService(CConfiguration.create(), runnerFactory, null, new NoOpProgramStateWriter()) {
      @Override
      public ProgramLiveInfo getLiveInfo(ProgramId programId) {
        return new ProgramLiveInfo(programId, "runtime") { };
      }

      @Override
      protected Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                      ProgramDescriptor programDescriptor,
                                      ArtifactDetail artifactDetail, File tempDir) throws IOException {
        return program;
      }

      @Override
      protected ArtifactDetail getArtifactDetail(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
        io.cdap.cdap.api.artifact.ArtifactId id = new io.cdap.cdap.api.artifact.ArtifactId(
          "dummy", new ArtifactVersion("1.0"), ArtifactScope.USER);
        return new ArtifactDetail(new ArtifactDescriptor(id, Locations.toLocation(TEMP_FOLDER.newFile())),
                                  new ArtifactMeta(ArtifactClasses.builder().build()));
      }
    };

    runtimeService.startAndWait();
    try {
      ProgramDescriptor descriptor = new ProgramDescriptor(program.getId(), null,
                                                           NamespaceId.DEFAULT.artifact("test", "1.0"));
      final ProgramController controller =
        runtimeService.run(descriptor, new SimpleProgramOptions(program.getId()), RunIds.generate()).getController();
      Tasks.waitFor(ProgramController.State.COMPLETED, controller::getState,
                    5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

      Tasks.waitFor(true, () ->  runtimeService.list(ProgramType.WORKER).isEmpty(),
                    5, TimeUnit.SECONDS, 100, TimeUnit.MICROSECONDS);
    } finally {
      runtimeService.stopAndWait();
    }
  }

  @Test (timeout = 5000L)
  public void testUpdateDeadLock() {
    // This test is for testing (CDAP-3716)
    // Create a service to simulate an existing running app.
    Service service = new TestService();
    ProgramId programId = NamespaceId.DEFAULT.app("dummyApp").program(ProgramType.WORKER, "dummy");
    RunId runId = RunIds.generate();
    ProgramRuntimeService.RuntimeInfo extraInfo = createRuntimeInfo(service, programId.run(runId));
    service.startAndWait();

    ProgramRunnerFactory runnerFactory = createProgramRunnerFactory();
    TestProgramRuntimeService runtimeService = new TestProgramRuntimeService(CConfiguration.create(),
                                                                             runnerFactory, null, extraInfo);
    runtimeService.startAndWait();

    // The lookup will get deadlock for CDAP-3716
    Assert.assertNotNull(runtimeService.lookup(programId, runId));
    service.stopAndWait();

    runtimeService.stopAndWait();
  }

  @Test
  public void testScopingRuntimeArguments() throws Exception {
    Map<ProgramId, Arguments> argumentsMap = new ConcurrentHashMap<>();
    ProgramRunnerFactory runnerFactory = createProgramRunnerFactory(argumentsMap);

    final Program program = createDummyProgram();
    final ProgramRuntimeService runtimeService =
      new AbstractProgramRuntimeService(CConfiguration.create(), runnerFactory, null, new NoOpProgramStateWriter()) {
      @Override
      public ProgramLiveInfo getLiveInfo(ProgramId programId) {
        return new ProgramLiveInfo(programId, "runtime") { };
      }

      @Override
      protected Program createProgram(CConfiguration cConf, ProgramRunner programRunner,
                                      ProgramDescriptor programDescriptor,
                                      ArtifactDetail artifactDetail, File tempDir) throws IOException {
        return program;
      }

      @Override
      protected ArtifactDetail getArtifactDetail(ArtifactId artifactId) throws IOException, ArtifactNotFoundException {
        io.cdap.cdap.api.artifact.ArtifactId id = new io.cdap.cdap.api.artifact.ArtifactId(
          "dummy", new ArtifactVersion("1.0"), ArtifactScope.USER);
        return new ArtifactDetail(new ArtifactDescriptor(id, Locations.toLocation(TEMP_FOLDER.newFile())),
                                  new ArtifactMeta(ArtifactClasses.builder().build()));
      }
    };

    runtimeService.startAndWait();
    try {
      try {
        ProgramDescriptor descriptor = new ProgramDescriptor(program.getId(), null,
                                                             NamespaceId.DEFAULT.artifact("test", "1.0"));

        // Set of scopes to test
        String programScope = program.getType().getScope();
        String clusterName = "c1";
        List<String> scopes = Arrays.asList(
          "cluster.*.",
          "cluster." + clusterName + ".",
          "cluster." + clusterName + ".app.*.",
          "app.*.",
          "app." + program.getApplicationId() + ".",
          "app." + program.getApplicationId() + "." + programScope + ".*.",
          "app." + program.getApplicationId() + "." + programScope + "." + program.getName() + ".",
          programScope + ".*.",
          programScope + "." + program.getName() + ".",
          ""
        );

        for (String scope : scopes) {
          ProgramOptions programOptions = new SimpleProgramOptions(
            program.getId(), new BasicArguments(Collections.singletonMap(Constants.CLUSTER_NAME, clusterName)),
            new BasicArguments(Collections.singletonMap(scope + "size", Integer.toString(scope.length()))));

          final ProgramController controller = runtimeService.run(descriptor, programOptions, RunIds.generate())
            .getController();
          Tasks.waitFor(ProgramController.State.COMPLETED, controller::getState,
                        5, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

          // Should get an argument
          Arguments args = argumentsMap.get(program.getId());
          Assert.assertNotNull(args);
          Assert.assertEquals(scope.length(), Integer.parseInt(args.getOption("size")));
        }

      } finally {
        runtimeService.stopAndWait();
      }

    } finally {
      runtimeService.stopAndWait();
    }
  }

  private ProgramRunnerFactory createProgramRunnerFactory() {
    return createProgramRunnerFactory(new HashMap<>());
  }

  /**
   * Creates a {@link ProgramRunnerFactory} for creating {@link ProgramRunner}
   * that always run with a {@link FastService}.
   *
   * @param argumentsMap the map to be populated with the user arguments for each run.
   */
  private ProgramRunnerFactory createProgramRunnerFactory(final Map<ProgramId, Arguments> argumentsMap) {
    return programType -> (program, options) -> {
      ProgramId programId = program.getId();
      argumentsMap.put(programId, options.getUserArguments());

      Service service = new FastService();
      ProgramController controller = new ProgramControllerServiceAdapter(service,
                                                                         programId.run(RunIds.generate()));
      service.start();
      return controller;
    };
  }

  private Program createDummyProgram() throws IOException {
    return new Program() {
      @Override
      public String getMainClassName() {
        return null;
      }

      @Override
      public <T> Class<T> getMainClass() throws ClassNotFoundException {
        return null;
      }

      @Override
      public ProgramType getType() {
        return ProgramType.WORKER;
      }

      @Override
      public ProgramId getId() {
        return new ApplicationId(NamespaceId.DEFAULT.getNamespace(), "dummyApp").worker("dummy");
      }

      @Override
      public String getName() {
        return getId().getProgram();
      }

      @Override
      public String getNamespaceId() {
        return getId().getNamespace();
      }

      @Override
      public String getApplicationId() {
        return getId().getApplication();
      }

      @Override
      public ApplicationSpecification getApplicationSpecification() {
        return null;
      }

      @Override
      public Location getJarLocation() {
        return null;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public void close() throws IOException {
        // No-op
      }
    };
  }

  private ProgramRuntimeService.RuntimeInfo createRuntimeInfo(Service service,
                                                              final ProgramRunId programRunId) {
    final ProgramControllerServiceAdapter controller =
      new ProgramControllerServiceAdapter(service, programRunId);
    return new ProgramRuntimeService.RuntimeInfo() {
      @Override
      public ProgramController getController() {
        return controller;
      }

      @Override
      public ProgramType getType() {
        return programRunId.getType();
      }

      @Override
      public ProgramId getProgramId() {
        return programRunId.getParent();
      }

      @Nullable
      @Override
      public RunId getTwillRunId() {
        return null;
      }
    };
  }

  /**
   * A guava service that has wil be completed immediate right after it is started.
   */
  private static final class FastService extends AbstractExecutionThreadService {

    @Override
    protected void run() throws Exception {
      // No-op
    }
  }

  /**
   * A guava service that will not terminate until stop is called.
   */
  private static final class TestService extends AbstractIdleService {

    @Override
    protected void startUp() throws Exception {
      // no-op
    }

    @Override
    protected void shutDown() throws Exception {
      // no-op
    }
  }

  /**
   * A runtime service that will insert an extra RuntimeInfo for the lookup call to simulate the situation
   * where an app is already running when the service starts.
   */
  private static final class TestProgramRuntimeService extends AbstractProgramRuntimeService {

    private final RuntimeInfo extraInfo;

    protected TestProgramRuntimeService(CConfiguration cConf, ProgramRunnerFactory programRunnerFactory,
                                        @Nullable ArtifactRepository artifactRepository,
                                        @Nullable RuntimeInfo extraInfo) {
      super(cConf, programRunnerFactory, artifactRepository, new NoOpProgramStateWriter());
      this.extraInfo = extraInfo;
    }

    @Override
    public ProgramLiveInfo getLiveInfo(ProgramId programId) {
      return new ProgramLiveInfo(programId, "runtime") { };
    }

    @Override
    public RuntimeInfo lookup(ProgramId programId, RunId runId) {
      RuntimeInfo info = super.lookup(programId, runId);
      if (info != null) {
        return info;
      }

      if (extraInfo != null) {
        updateRuntimeInfo(programId.getType(), runId, extraInfo);
        return extraInfo;
      }
      return null;
    }
  }
}
