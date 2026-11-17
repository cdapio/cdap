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

import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.io.Locations;
import io.cdap.cdap.common.ssh.SSHConfig;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link RemoteExecutionTwillPreparer}.
 */
public class RemoteExecutionTwillPreparerTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Minimal runnable for {@link TwillSpecification} construction.
   */
  public static final class NoOpRunnable extends AbstractTwillRunnable {
    @Override
    public void run() {
      // no-op
    }
  }

  @Test
  public void testLocalizeFilesUsesDeterministicMd5ForAsciiAndUnicodeUrisAndDeduplicates()
      throws Exception {
    File asciiFile = tempFolder.newFile("config-alpha.txt");
    byte[] asciiBytes = "alpha-content".getBytes(StandardCharsets.UTF_8);
    Files.write(asciiFile.toPath(), asciiBytes);

    File unicodeDir = tempFolder.newFolder("dir_üñîçødé");
    File unicodeArchive = new File(unicodeDir, "archive_üñîçødé.jar");
    byte[] archiveBytes = "archive-content".getBytes(StandardCharsets.UTF_8);
    Files.write(unicodeArchive.toPath(), archiveBytes);

    URI asciiUri = asciiFile.toURI();
    URI unicodeUri = unicodeArchive.toURI();

    Map<String, LocalFile> localFiles = new LinkedHashMap<>();
    localFiles.put(
        "alpha.txt",
        new DefaultLocalFile(
            "alpha.txt", asciiUri, asciiFile.lastModified(), asciiBytes.length, false, null));
    // Same URI mapped to a second target name must only upload once to .localized
    localFiles.put(
        "alpha-copy.txt",
        new DefaultLocalFile(
            "alpha-copy.txt", asciiUri, asciiFile.lastModified(), asciiBytes.length, false, null));

    RuntimeSpecification runtimeSpec = Mockito.mock(RuntimeSpecification.class);
    LocalFile unicodeLocalFile = new DefaultLocalFile(
        "expanded-archive",
        unicodeUri,
        unicodeArchive.lastModified(),
        archiveBytes.length,
        true,
        null);
    Mockito.when(runtimeSpec.getLocalFiles())
        .thenReturn(Collections.singletonList(unicodeLocalFile));

    RemoteExecutionTwillPreparer preparer = createPreparer();
    Assert.assertEquals(
        Constants.Files.APPLICATION_JAR, preparer.getApplicationJarLocalizedName("ignored-hash"));

    SSHSession session = Mockito.mock(SSHSession.class);
    Mockito.when(session.getUsername()).thenReturn("cdap");
    Mockito.when(session.getAddress())
        .thenReturn(InetSocketAddress.createUnresolved("127.0.0.1", 22));

    List<String> uploadedFileNames = new ArrayList<>();
    Mockito.doAnswer(invocation -> {
      uploadedFileNames.add(invocation.getArgument(2));
      return null;
    }).when(session).copy(
        Mockito.any(InputStream.class),
        Mockito.eq("/remote/run/.localized"),
        Mockito.anyString(),
        Mockito.anyLong(),
        Mockito.anyInt(),
        Mockito.any(),
        Mockito.any());

    Method localizeFilesMethod = RemoteExecutionTwillPreparer.class.getDeclaredMethod(
        "localizeFiles", SSHSession.class, Map.class, String.class, RuntimeSpecification.class);
    localizeFilesMethod.setAccessible(true);
    localizeFilesMethod.invoke(preparer, session, localFiles, "/remote/run", runtimeSpec);

    String expectedAsciiLocalized = computeUnencodedMd5Hex(asciiUri.toString()) + "-config-alpha.txt";
    String expectedUnicodeLocalized =
        computeUnencodedMd5Hex(unicodeUri.toString()) + "-archive_üñîçødé.jar";

    Assert.assertEquals(2, uploadedFileNames.size());
    Assert.assertEquals(expectedAsciiLocalized, uploadedFileNames.get(0));
    Assert.assertEquals(expectedUnicodeLocalized, uploadedFileNames.get(1));

    Mockito.verify(session).executeAndWait(
        "ln /remote/run/.localized/" + expectedAsciiLocalized + " /remote/run/alpha.txt");
    Mockito.verify(session).executeAndWait(
        "ln /remote/run/.localized/" + expectedAsciiLocalized + " /remote/run/alpha-copy.txt");
    Mockito.verify(session).executeAndWait(
        "mkdir -p /remote/run/expanded-archive",
        "cd /remote/run/expanded-archive",
        "jar xf /remote/run/.localized/" + expectedUnicodeLocalized);
  }

  private RemoteExecutionTwillPreparer createPreparer() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    LocalLocationFactory locationFactory =
        new LocalLocationFactory(tempFolder.newFolder("locations"));
    LocationCache locationCache =
        new BasicLocationCache(Locations.toLocation(tempFolder.newFolder("cache")));

    ProgramId programId = NamespaceId.DEFAULT.app("app").spark("sparkJob");
    ProgramRunId runId = programId.run(RunIds.generate());
    ProgramOptions programOptions =
        new SimpleProgramOptions(programId, new BasicArguments(), new BasicArguments());
    TwillSpecification twillSpec = new SingleRunnableApplication(
        new NoOpRunnable(), ResourceSpecification.BASIC).configure();

    Cluster cluster = new Cluster(
        "test-cluster", ClusterStatus.RUNNING, Collections.emptyList(), Collections.emptyMap());
    SSHConfig sshConfig = SSHConfig.builder("127.0.0.1")
        .setUser("cdap")
        .setPrivateKeySupplier(() -> "dummy-key".getBytes(StandardCharsets.UTF_8))
        .build();

    return new RemoteExecutionTwillPreparer(
        cConf,
        new Configuration(),
        cluster,
        sshConfig,
        Collections.emptyMap(),
        twillSpec,
        runId,
        programOptions,
        locationCache,
        locationFactory,
        (startupTask, timeout, unit) -> Mockito.mock(TwillController.class));
  }

  private static String computeUnencodedMd5Hex(String value) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    byte[] digest = md5.digest(value.getBytes(StandardCharsets.UTF_16LE));
    StringBuilder hex = new StringBuilder(digest.length * 2);
    for (byte b : digest) {
      hex.append(String.format("%02x", b & 0xff));
    }
    return hex.toString();
  }
}
