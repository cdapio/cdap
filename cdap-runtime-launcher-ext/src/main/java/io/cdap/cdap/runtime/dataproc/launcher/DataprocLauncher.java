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

package io.cdap.cdap.runtime.dataproc.launcher;

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
import com.google.gson.Gson;
import io.cdap.cdap.runtime.spi.launcher.JobId;
import io.cdap.cdap.runtime.spi.launcher.LaunchInfo;
import io.cdap.cdap.runtime.spi.launcher.Launcher;
import io.cdap.cdap.runtime.spi.launcher.LauncherFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DataprocLauncher implements Launcher {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocLauncher.class);
  private static final Gson GSON = new Gson();

  @Override
  public String getName() {
    return "dataproc-launcher";
  }

  @Override
  public JobId launch(LaunchInfo info) {
    String fileSuffix = null;
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
      // TODO Get artifacts to run mapreduce or spark jobs
      List<String> uris = new ArrayList<>();
      List<String> files = new ArrayList<>();

      for (LauncherFile launcherFile : info.getLauncherFileList()) {
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
        Blob blob = storage.create(blobInfo, bytesArray);

        if (launcherFile.getName().endsWith("jar")) {
          LOG.info("Adding {} to jar", blob.getName());
          uris.add("gs://launcher-three/" + blob.getName());
          if (blob.getName().startsWith("expanded.")) {
            // 2.0.0-SNAPSHOT.06ff0c10-0129-4c8b-aa13-03b958b408cf.jar to jar
            int i = blob.getName().lastIndexOf(".jar");
            fileSuffix = blob.getName().substring(0, i);
            fileSuffix = fileSuffix.substring(fileSuffix.lastIndexOf('.') + 1);
            LOG.info("File suffix is : {}", fileSuffix);
          }
        } else {
          LOG.info("Adding {} to file", blob.getName());
          files.add("gs://launcher-three/" + blob.getName());
        }
      }

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
        SubmitJobRequest request = SubmitJobRequest.newBuilder()
          .setRegion("us-west1")
          .setProjectId("vini-project-238000")
          .setJob(Job.newBuilder().setPlacement(JobPlacement.newBuilder()
                                                  .setClusterName(info.getClusterName()).build())
                    .setHadoopJob(HadoopJob.newBuilder()
                                    .setMainClass("io.cdap.cdap.internal.app.runtime.distributed" +
                                                    ".launcher.WrappedLauncher")
                                    .addAllJarFileUris(uris)
                                    .addAllFileUris(files)
                                    .addAllArchiveUris(archive)
                                    .addAllArgs(ImmutableList.of(info.getProgramId(), fileSuffix))
                                    .build())
                    .build())
          .build();
        CredentialsProvider credentialsProvider = FixedCredentialsProvider
          .create(GoogleCredentials.getApplicationDefault());
        JobControllerClient client = JobControllerClient.create(
          JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
            .setEndpoint("us-west1-dataproc.googleapis.com:443").build()
        );

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
}
