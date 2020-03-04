/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.provisioner.dataproc;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.SubmitJobRequest;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.runtime.spi.launcher.JobId;
import io.cdap.cdap.runtime.spi.launcher.LaunchInfo;
import io.cdap.cdap.runtime.spi.launcher.Launcher;
import io.cdap.cdap.runtime.spi.launcher.LauncherFile;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 *
 */
public class DataprocLauncher implements Launcher {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocLauncher.class);

  @Override
  public String getClassName() {
    return DataprocLauncher.class.getName();
  }

  @Override
  public URI getJarURI() {
    Location location = null;
    try {
      Path cachePath = Files.createTempDirectory(Paths.get("/tmp").toAbsolutePath(), "launcher.cache");
      LocationCache locationCache = new BasicLocationCache(new LocalLocationFactory().create(cachePath.toUri()));
      location = locationCache.get("dataproclauncher.jar", new LocationCache.Loader() {
        @Override
        public void load(String name, Location targetLocation) throws IOException {
          // Create a jar file with the TwillLauncher and FindFreePort and dependent classes inside.
          ClassLoader classLoader = DataprocLauncher.class.getClassLoader();
          try (JarOutputStream jarOut = new JarOutputStream(targetLocation.getOutputStream())) {
            if (classLoader == null) {
              classLoader = getClass().getClassLoader();
            }
            Set<String> seen = new HashSet<>();
            Dependencies.findClassDependencies(classLoader, new ClassAcceptor() {
              @Override
              public boolean accept(String className, URL classUrl, URL classPathUrl) {
                if (className.startsWith("io.cdap.") || className.startsWith("io/cdap") ||
                  className.startsWith("org.apache.twill") || className.startsWith("org/apache/twill")) {
                  try {
                    jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
                    seen.add(className);
                    try (InputStream is = classUrl.openStream()) {
                      ByteStreams.copy(is, jarOut);
                    }
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return true;
                }
                return false;
              }
            }, DataprocJobMain.class.getName(), Services.class.getName());
          }
        }
      });
    } catch (IOException e) {
      LOG.error("### Error while creating launcher jar.", e);
    }

    return location.toURI();
  }

  @Override
  public JobId launch(LaunchInfo info) {
    try {

      LOG.info("Inside dataproc launcher for program id: {}", info.getProgramId());
      Map<String, String> properties = info.getProperties();
      LOG.info("region for dataproc cluster: {}", properties.get("system.properties.region"));
      LOG.info("name of dataproc cluster: {}", info.getClusterName());
      LOG.info("project id of dataproc cluster: {}", properties.get("system.properties.projectId"));

      // Instantiates a client
      Storage storage = StorageOptions.getDefaultInstance().getService();

      // The name for the new bucket
      String bucketName = "launcher-three";
      List<String> uris = new ArrayList<>();
      List<String> files = new ArrayList<>();

      // TODO parallelize this to be uploaded by multiple threads for faster job submission
      for (LauncherFile launcherFile : info.getLauncherFileList()) {
        LOG.info("Uploading file {}", launcherFile.getName());
        uploadFile(storage, bucketName, uris, files, launcherFile);
      }
      //   executor.awaitTermination(30, TimeUnit.MINUTES);

      List<String> archive = new ArrayList<>();

      for (LauncherFile launcherFile : info.getLauncherFileList()) {
        if (launcherFile.isArchive()) {
          LOG.info("Adding {} to archive", launcherFile.getName());
          String name = launcherFile.getName();
          archive.add(name);
        }
      }
      // TODO Get cluster information from provisioned cluster using Launcher interface
      try {
        SubmitJobRequest request;
        LOG.info("### Program type is: {}", info.getProgramType());
        LOG.info("Hadoop job request is created");
        request = SubmitJobRequest.newBuilder()
          .setRegion("us-west1")
          .setProjectId("vini-project-238000")
          .setJob(Job.newBuilder().setPlacement(JobPlacement.newBuilder()
                                                  .setClusterName(info.getClusterName()).build())
                    .setHadoopJob(HadoopJob.newBuilder()
                                    .setMainClass(DataprocJobMain.class.getName())
                                    .addAllJarFileUris(uris)
                                    .addAllFileUris(files)
                                    .addAllArchiveUris(archive)
                                    .addAllArgs(ImmutableList.of(info.getProgramId()))
                                    .build())
                    .build())
          .build();
        CredentialsProvider credentialsProvider = FixedCredentialsProvider
          .create(GoogleCredentials.getApplicationDefault());
        JobControllerClient client = JobControllerClient.create(
          JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
            .setEndpoint("us-west1-dataproc.googleapis.com:443").build()
        );

        LOG.info("Submitting hadoop job");
        Job job = client.submitJob(request);
        LOG.info("Successfully launched job on dataproc");
      } catch (Exception e) {
        LOG.error("Error while launching hadoop job on dataproc {}", e.getMessage(), e);
      }
    } catch (Exception e) {
      LOG.error("error while launching the job: " + e.getMessage(), e);
    }

    // TODO return job id for monitoring
    return null;
  }

  private void uploadFile(Storage storage, String bucketName, List<String> uris,
                            List<String> files, LauncherFile launcherFile) throws IOException {
    LOG.info("Uploading launcher file: {} to gcs bucket.", launcherFile.getName());

    File file = new File(launcherFile.getUri());
    //init array with file length
    byte[] bytesArray = new byte[(int) file.length()];

    FileInputStream fis = new FileInputStream(file);
    fis.read(bytesArray); //read file into bytes[]
    fis.close();

    String fileName = launcherFile.getName();

    if (fileName.equals("artifacts")) {
      fileName = "artifacts.jar";
    }
    BlobId blobId = BlobId.of(bucketName, fileName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
    // TODO use writer channel instead of create for large files. Figure out whether that api is atomic
    Blob blob = storage.create(blobInfo, bytesArray);

    if (launcherFile.getName().endsWith("jar") || launcherFile.getName().contains("cConf.xml")) {
      LOG.info("Adding {} to jar", blob.getName());
      uris.add("gs://launcher-three/" + blob.getName());
    } else {
      LOG.info("Adding {} to file", blob.getName());
      files.add("gs://launcher-three/" + blob.getName());
    }
  }
}
