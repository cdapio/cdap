/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.utils.ProjectInfo;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.ProgramOptionConstants;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.TwillRuntimeSpecification;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link AbstractRuntimeTwillPreparer}.
 */
public class AbstractRuntimeTwillPreparerTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Minimal ASCII runnable for deterministic application jar hashing.
   */
  public static final class SampleAsciiRunnable extends AbstractTwillRunnable {
    @Override
    public void run() {
      // no-op
    }
  }

  /**
   * Extra dependency class to verify deterministic multi-class MD5 hashing.
   */
  public static final class ExtraDependencyClass {
  }

  @Test
  public void testStartComputesDeterministicApplicationJarMd5AndInvokesLaunch() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    File dataDir = tempFolder.newFolder("data");
    cConf.set(
        io.cdap.cdap.common.conf.Constants.CFG_LOCAL_DATA_DIR,
        dataDir.getAbsolutePath());
    Assert.assertTrue(
        new File(dataDir, cConf.get(io.cdap.cdap.common.conf.Constants.AppFabric.TEMP_DIR))
            .mkdirs());

    LocalLocationFactory locationFactory =
        new LocalLocationFactory(tempFolder.newFolder("locations"));
    Location cacheDir = Locations.toLocation(tempFolder.newFolder("cache"));
    LocationCache locationCache = new BasicLocationCache(cacheDir);

    ProgramId programId = NamespaceId.DEFAULT.app("sparkApp").spark("sparkJob");
    ProgramRunId runId = programId.run(RunIds.generate());
    ProgramOptions programOptions = new SimpleProgramOptions(
        programId,
        new BasicArguments(ImmutableMap.of(ProgramOptionConstants.RUN_ID, runId.getRun())),
        new BasicArguments());

    TwillSpecification twillSpec = new SingleRunnableApplication(
        new SampleAsciiRunnable(), ResourceSpecification.BASIC).configure();
    TwillController expectedController = Mockito.mock(TwillController.class);
    TwillControllerFactory syncFactory = (startupTask, timeout, timeoutUnit) -> {
      try {
        if (startupTask != null) {
          startupTask.call();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return expectedController;
    };

    AtomicReference<String> capturedHashVal = new AtomicReference<>();
    AtomicReference<Map<String, LocalFile>> capturedLocalFiles = new AtomicReference<>();

    AbstractRuntimeTwillPreparer preparer = new TestRuntimeTwillPreparer(
        cConf,
        new Configuration(),
        twillSpec,
        runId,
        programOptions,
        locationCache,
        locationFactory,
        syncFactory,
        capturedHashVal,
        capturedLocalFiles);

    preparer.withDependencies(ExtraDependencyClass.class);
    preparer.withBundlerClassAcceptor(new ClassAcceptor() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        return false;
      }
    });
    TwillController actualController = preparer.start(30, TimeUnit.SECONDS);

    Assert.assertSame(expectedController, actualController);

    List<String> expectedSortedClassNames = Arrays.asList(
            SampleAsciiRunnable.class.getName(),
            ExtraDependencyClass.class.getName())
        .stream()
        .sorted()
        .collect(Collectors.toList());
    String expectedMd5 = computeUnencodedMd5Hex(
        expectedSortedClassNames, ProjectInfo.getVersion().toString());

    Assert.assertEquals(expectedMd5, capturedHashVal.get());
    Map<String, LocalFile> localFiles = capturedLocalFiles.get();
    Assert.assertNotNull(localFiles);
    LocalFile appJar = localFiles.get(Constants.Files.APPLICATION_JAR);
    Assert.assertNotNull(appJar);
    Assert.assertEquals(expectedMd5 + "-custom-app.jar", appJar.getName());
    Assert.assertTrue(cacheDir.append(expectedMd5 + "-" + Constants.Files.APPLICATION_JAR).exists());
  }

  private static String computeUnencodedMd5Hex(
      List<String> sortedClassNames, String versionString) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    for (String className : sortedClassNames) {
      md5.update(className.getBytes(StandardCharsets.UTF_16LE));
    }
    md5.update(versionString.getBytes(StandardCharsets.UTF_16LE));
    byte[] digest = md5.digest();
    StringBuilder hex = new StringBuilder(digest.length * 2);
    for (byte b : digest) {
      hex.append(String.format("%02x", b & 0xff));
    }
    return hex.toString();
  }

  private static final class TestRuntimeTwillPreparer extends AbstractRuntimeTwillPreparer {

    private final AtomicReference<String> capturedHashVal;
    private final AtomicReference<Map<String, LocalFile>> capturedLocalFiles;

    private TestRuntimeTwillPreparer(
        CConfiguration cConf,
        Configuration hConf,
        TwillSpecification twillSpec,
        ProgramRunId programRunId,
        ProgramOptions programOptions,
        LocationCache locationCache,
        LocationFactory locationFactory,
        TwillControllerFactory controllerFactory,
        AtomicReference<String> capturedHashVal,
        AtomicReference<Map<String, LocalFile>> capturedLocalFiles) {
      super(
          cConf,
          hConf,
          twillSpec,
          programRunId,
          programOptions,
          locationCache,
          locationFactory,
          controllerFactory);
      this.capturedHashVal = capturedHashVal;
      this.capturedLocalFiles = capturedLocalFiles;
    }

    @Override
    String getApplicationJarLocalizedName(String hashVal) {
      capturedHashVal.set(hashVal);
      return hashVal + "-custom-app.jar";
    }

    @Override
    void launch(
        TwillRuntimeSpecification twillRuntimeSpec,
        RuntimeSpecification runtimeSpec,
        JvmOptions jvmOptions,
        Map<String, String> environments,
        Map<String, LocalFile> localFiles,
        TimeoutChecker timeoutChecker) {
      capturedLocalFiles.set(Collections.unmodifiableMap(localFiles));
    }
  }
}
