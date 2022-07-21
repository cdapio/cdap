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

package io.cdap.cdap.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.cdap.cdap.StandaloneTester;
import io.cdap.cdap.cli.util.InstanceURIParser;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.common.ProgramNotFoundException;
import io.cdap.cdap.common.test.AppJarHelper;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.ProgramRecord;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.ProgramStatus;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.security.spi.authentication.UnauthenticatedException;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.jar.Manifest;

/**
 * Base class for writing client unit-test. It provides common functionality for writing client tests.
 */
public abstract class AbstractClientTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder() {

    private int refCount;

    @Override
    protected void before() throws Throwable {
      if (refCount++ == 0) {
        super.before();
      }
    }

    @Override
    protected void after() {
      if (--refCount == 0) {
        super.after();
      }
    }
  };

  protected ClientConfig clientConfig;

  protected abstract StandaloneTester getStandaloneTester();

  private static final LoadingCache<ArtifactJarInfo, Location> ARTIFACT_CACHE =
    CacheBuilder.newBuilder().build(new CacheLoader<ArtifactJarInfo, Location>() {
      @Override
      public Location load(ArtifactJarInfo key) throws Exception {
        File tmpJarFolder = TMP_FOLDER.newFolder();
        LocationFactory locationFactory = new LocalLocationFactory(tmpJarFolder);
        return AppJarHelper.createDeploymentJar(locationFactory, key.getAppClass(), key.getManifest());
      }
    });

  @Before
  public void setUp() throws Throwable {
    StandaloneTester standalone = getStandaloneTester();
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(standalone.getBaseURI().toString());
    clientConfig = new ClientConfig.Builder()
      .setVerifySSLCert(false)
      .setDefaultReadTimeout(60 * 1000)
      .setUploadReadTimeout(120 * 1000)
      .setConnectionConfig(connectionConfig).build();
  }

  protected ClientConfig getClientConfig() {
    return clientConfig;
  }

  protected void verifyProgramNames(List<String> expected, List<ProgramRecord> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (ProgramRecord actualProgram : actual) {
      Assert.assertTrue(expected.contains(actualProgram.getName()));
    }
  }

  protected void verifyProgramRecords(List<String> expected, Map<ProgramType, List<ProgramRecord>> map) {
    verifyProgramNames(expected, Lists.newArrayList(Iterables.concat(map.values())));
  }

  protected void assertProgramRunning(ProgramClient programClient, ProgramId program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException,
    InterruptedException, UnauthorizedException {

    assertProgramStatus(programClient, program, ProgramStatus.RUNNING);
  }

  protected void assertProgramStopped(ProgramClient programClient, ProgramId program)
    throws IOException, ProgramNotFoundException, UnauthenticatedException,
    InterruptedException, UnauthorizedException {

    assertProgramStatus(programClient, program, ProgramStatus.STOPPED);
  }

  protected void assertProgramStatus(ProgramClient programClient, ProgramId program, ProgramStatus programStatus)
    throws IOException, ProgramNotFoundException, UnauthenticatedException,
    InterruptedException, UnauthorizedException {

    try {
      programClient.waitForStatus(program, programStatus, 60, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // NO-OP
    }

    Assert.assertEquals(programStatus.name(), programClient.getStatus(program));
  }

  protected void assertProgramRuns(final ProgramClient programClient, final ProgramId program,
                                   final ProgramRunStatus programRunStatus, final int expected, int timeout)
    throws InterruptedException, ExecutionException, TimeoutException {
    Tasks.waitFor(expected, new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        return programClient.getProgramRuns(program, programRunStatus.name(),
                                            0, Long.MAX_VALUE, Integer.MAX_VALUE).size();
      }
    }, timeout, TimeUnit.SECONDS);
  }

  protected File createAppJarFile(Class<?> cls) throws IOException {
    return createArtifactJarFile(cls, new Manifest());
  }

  protected File createAppJarFile(Class<?> cls, String name, String version) throws IOException {
    return createArtifactJarFile(cls, name, version, new Manifest());
  }

  protected File createArtifactJarFile(Class<?> cls, Manifest manifest) throws IOException {
    return createArtifactJarFile(cls, cls.getSimpleName(),
                                 String.format("1.0.%d-SNAPSHOT", System.currentTimeMillis()), manifest);
  }

  protected File createArtifactJarFile(Class<?> cls, String name,
                                       String version, Manifest manifest) throws IOException {
    Location deploymentJar = ARTIFACT_CACHE.getUnchecked(new ArtifactJarInfo(cls, manifest));

    File appJarFile = new File(TMP_FOLDER.newFolder(), String.format("%s-%s.jar", name, version));
    try {
      Files.createLink(appJarFile.toPath(), Paths.get(deploymentJar.toURI()));
    } catch (Exception e) {
      Files.copy(appJarFile.toPath(), Paths.get(deploymentJar.toURI()));
    }
    return appJarFile;
  }

  private static final class ArtifactJarInfo {
    private final Class<?> appClass;
    private final Manifest manifest;

    private ArtifactJarInfo(Class<?> appClass, Manifest manifest) {
      this.appClass = appClass;
      this.manifest = manifest;
    }

    Class<?> getAppClass() {
      return appClass;
    }

    Manifest getManifest() {
      return manifest;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ArtifactJarInfo that = (ArtifactJarInfo) o;
      return Objects.equals(appClass, that.appClass) &&
        Objects.equals(manifest, that.manifest);
    }

    @Override
    public int hashCode() {
      return Objects.hash(appClass, manifest);
    }
  }
}
