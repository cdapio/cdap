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
import io.cdap.cdap.runtime.runtimejob.DataprocJobMain;
import io.cdap.cdap.runtime.runtimejob.DatarpocJarUtils;
import io.cdap.cdap.runtime.spi.launcher.JobId;
import io.cdap.cdap.runtime.spi.launcher.LaunchInfo;
import io.cdap.cdap.runtime.spi.launcher.Launcher;
import io.cdap.cdap.runtime.spi.launcher.LauncherFile;
import joptsimple.OptionSpec;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.ApplicationBundler;
import org.apache.twill.internal.appmaster.ApplicationMasterMain;
import org.apache.twill.internal.container.TwillContainerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

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
   return null;
  }

  @Override
  public JobId launch(LaunchInfo info) {
    try {
      LOG.info("Inside dataproc launcher for program id: {}", info.getProgramId());

      LocationFactory locationFactory = new LocalLocationFactory(Files.createTempDirectory("locallocation").toFile());
      ApplicationBundler bundler = DatarpocJarUtils.createBundler();

      // build twill jar
      LocalFile twillJar = DatarpocJarUtils.getBundleJar(bundler, locationFactory, "twill.jar",
                                                         ImmutableList.of(ApplicationMasterMain.class,
                                                                          TwillContainerMain.class, OptionSpec.class));

      // build application jar
      LocalFile runtimeJobJar = DatarpocJarUtils.getBundleJar(bundler, locationFactory, "application.jar",
                                                              ImmutableList.of(info.getRuntimeJobClass()));

      // build thin launcher jar using dependency tracing. Only allow cdap classes
      LocalFile launcherJar = DatarpocJarUtils.getBundleJar(bundler, locationFactory, "launcher.jar",
                                                            ImmutableList.of(DataprocJobMain.class));

      // Instantiates a client
      Storage storage = StorageOptions.getDefaultInstance().getService();

      // The name for the new bucket
      String bucketName = "launcher-three";
      List<String> uris = new ArrayList<>();
      List<String> files = new ArrayList<>();

      // TODO parallelize this to be uploaded by multiple threads for faster job submission
      for (LauncherFile launcherFile : info.getLauncherFileList()) {
        LOG.info("Uploading file {}", launcherFile.getName());
        if (launcherFile.getName().equals("twill.jar") || launcherFile.getName().equals("application.jar")) {
          continue;
        }
        uploadFile(storage, bucketName, uris, files, launcherFile);
      }

      uploadFile(storage, bucketName, uris, files,
                 new LauncherFile(runtimeJobJar.getName(), runtimeJobJar.getURI(), false));
      uploadFile(storage, bucketName, uris, files,
                 new LauncherFile(twillJar.getName(), twillJar.getURI(), false));
      uploadFile(storage, bucketName, uris, files,
                 new LauncherFile(launcherJar.getName(), launcherJar.getURI(), false));

      List<String> archive = new ArrayList<>();

      for (LauncherFile launcherFile : info.getLauncherFileList()) {
        if (launcherFile.isArchive()) {
          LOG.info("Adding {} to archive", launcherFile.getName());
          String name = launcherFile.getName();
          archive.add("gs://launcher-three/" + name);
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
                                    .addAllArgs(ImmutableList.of(info.getRuntimeJobClass().getName()))
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

    if (launcherFile.getName().endsWith("jar")) {
      LOG.info("Adding {} to jar", fileName);
      uris.add("gs://launcher-three/" + fileName);
    } else {
      LOG.info("Adding {} to file", fileName);
      files.add("gs://launcher-three/" + fileName);
    }

    if (fileName.equals("artifacts.jar") || fileName.equals("cConf.xml")
      || fileName.equals("runtime.config.jar") || fileName.equals("logback.xml") ||
      fileName.equals("resources.jar") || fileName.equals("artifacts_archive.jar")) {
      //fileName = "artifacts.jar";
      LOG.info("not adding file to gcs, skipping {}", fileName);
      return;
    }
    BlobId blobId = BlobId.of(bucketName, fileName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
    Blob blob = storage.create(blobInfo, bytesArray);
  }
}
