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

package io.cdap.cdap.runtime.runtimejob;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.SubmitJobRequest;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.runtime.spi.runtimejob.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobDetail;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobId;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobInfo;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobStatus;
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
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Dataproc runtime job manager.
 */
public class DataprocRuntimeJobManager implements RuntimeJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocRuntimeJobManager.class);
  private final String clusterName;
  private final String projectId;
  private final String region;
  private final String bucket;
  private final String sparkVersion;
  private final Map<String, String> labels;

  private Storage gcsStorage;
  private LocationFactory localLocationFactory;
  private CredentialsProvider credentialsProvider;
  private JobControllerClient jobControllerClient;

  /**
   * This instance is created by provisioner which is responsible for providing information needed by runtime job
   * manager.
   */
  public DataprocRuntimeJobManager(String clusterName, String projectId, String region, String bucket,
                                   String sparkVersion, Map<String, String> labels) {
    this.clusterName = clusterName;
    this.projectId = projectId;
    this.region = region;
    this.bucket = bucket;
    this.sparkVersion = sparkVersion;
    this.labels = new HashMap<>(labels);

    try {
      // TODO: move this to seperate class
      this.localLocationFactory = new LocalLocationFactory(Files.createTempDirectory("local.launch.location").toFile());
      // Instantiates gcs client
      this.gcsStorage = StorageOptions.getDefaultInstance().getService();
      this.credentialsProvider = FixedCredentialsProvider
        .create(GoogleCredentials.getApplicationDefault());
      this.jobControllerClient = JobControllerClient.create(
        JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
          .setEndpoint(region + "-dataproc.googleapis.com:443").build());

    } catch (IOException e) {
      LOG.info("Location factory could not be created.", e);
    }
  }

  @Override
  public RuntimeJobId launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    ProgramRunInfo info = runtimeJobInfo.getProgramRunInfo();
    String gcsPath = getBasePath(info.getNamespace(), info.getApplication(), info.getVersion(), info.getProgram(),
                                 info.getRun());

    // Create jars for the launch
    ApplicationBundler bundler = DatarpocJarUtils.createBundler();
    // build twill jar
    LocalFile twillJar = DatarpocJarUtils.getBundleJar(bundler, localLocationFactory, "twill.jar",
                                                       ImmutableList.of(ApplicationMasterMain.class,
                                                                        TwillContainerMain.class, OptionSpec.class));
    // build application jar
    LocalFile runtimeJobJar = DatarpocJarUtils.getBundleJar(bundler, localLocationFactory, "application.jar",
                                                            ImmutableList.of(runtimeJobInfo.getRuntimeJobClass()));

    // build thin launcher jar
    LocalFile launcherJar = DatarpocJarUtils.getBundleJar(bundler, localLocationFactory, "launcher.jar",
                                                          ImmutableList.of(DataprocJobMain.class));

    List<LocalFile> localFiles = new ArrayList<>(runtimeJobInfo.getLocalizeFiles());
    localFiles.add(twillJar);
    localFiles.add(runtimeJobJar);
    localFiles.add(launcherJar);

    Set<String> jars = new HashSet<>();
    Set<String> files = new HashSet<>();
    Set<String> archives = new HashSet<>();

    for (LocalFile localFile : localFiles) {
      String fileName = localFile.getName();
      // artifacts and artifacts_archive are same jars. So skip artifacts_archive jar
      if (fileName.equals("artifacts_archive")) {
        continue;
      }
      // artifacts jar is constructed with name artifacts.
      if (fileName.equals("artifacts")) {
        fileName = fileName + ".jar";
      }
      uploadFile(gcsStorage, gcsPath, fileName, localFile);

      if (localFile.isArchive()) {
        LOG.debug("Adding {} to archive", localFile.getName());
        String name = localFile.getName();
        archives.add(name);
      }

      if (localFile.getName().endsWith("jar")) {
        LOG.info("Adding {} to jar", fileName);
        jars.add(gcsPath + fileName);
      } else {
        LOG.info("Adding {} to file", fileName);
        files.add(gcsPath + fileName);
      }
    }

    SubmitJobRequest request;
    request = SubmitJobRequest.newBuilder()
      .setRegion(region)
      .setProjectId(projectId)
      .setJob(Job.newBuilder().setPlacement(JobPlacement.newBuilder()
                                              .setClusterName(clusterName).build())
                // add same labels as dataproc cluster, also add program related information
                .putAllLabels(labels)
                .putLabels("namespace", info.getNamespace())
                .putLabels("application", info.getNamespace())
                .putLabels("version", info.getNamespace())
                .putLabels("program", info.getNamespace())
                .putLabels("program", info.getNamespace())
                .putLabels("run", info.getRun())
                .setHadoopJob(HadoopJob.newBuilder()
                                // set main class
                                .setMainClass(DataprocJobMain.class.getName())
                                // set all the jar
                                .addAllJarFileUris(jars)
                                // set all the files
                                .addAllFileUris(files)
                                // set all the archive files
                                .addAllArchiveUris(archives)
                                // set main class arguments
                                .addAllArgs(ImmutableList.of(runtimeJobInfo.getRuntimeJobClass().getName(),
                                                             sparkVersion))
                                .build())
                .build())
      .build();

    LOG.info("Submitting hadoop job....");

    Job job = jobControllerClient.submitJob(request);

    LOG.info("Successfully launched job on dataproc.");

    // TODO delete gcs files upon any exception before returning to app fabric
    return new RuntimeJobId(job.getJobUuid());
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(RuntimeJobId runtimeJobId) throws Exception {
    Optional<RuntimeJobDetail> jobDetails;
    String jobId = runtimeJobId.getRuntimeJobId();
    try {
      Job job = jobControllerClient.getJob(projectId, region, jobId);
      RuntimeJobStatus runtimeJobStatus = getRuntimeJobStatus(job);

      jobDetails = Optional.of(new RuntimeJobDetail(runtimeJobId, runtimeJobStatus));
    } catch (ApiException e) {
      // this may happen right after a job is created
      if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        jobDetails = Optional.empty();
      } else {
        throw e;
      }
    }
    return jobDetails;
  }

  @Override
  public Iterable<RuntimeJobDetail> list() throws Exception {
    Set<String> filters = new HashSet<>();
    filters.add("status.state = ACTIVE");
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      filters.add("labels." + entry.getKey() + " = " + entry.getValue());
    }

    JobControllerClient.ListJobsPagedResponse listJobsPagedResponse =
      jobControllerClient.listJobs(projectId, region, Joiner.on(" AND ").join(filters));

    Iterator<Job> jobsItor = listJobsPagedResponse.iterateAll().iterator();

    List<RuntimeJobDetail> jobsDetail = new ArrayList<>();
    while (jobsItor.hasNext()) {
      Job job = jobsItor.next();
      jobsDetail.add(new RuntimeJobDetail(new RuntimeJobId(job.getJobUuid()), getRuntimeJobStatus(job)));
    }
    return jobsDetail;
  }

  @Override
  public void stop(RuntimeJobId runtimeJobId) throws Exception {
    Optional<RuntimeJobDetail> jobDetail = getDetail(runtimeJobId);
    if (!jobDetail.isPresent()) {
      return;
    }
    String jobId = runtimeJobId.getRuntimeJobId();
    Job job = null;
    try {
      job = jobControllerClient.cancelJob(projectId, region, jobId);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.FAILED_PRECONDITION) {
        LOG.warn("Failed to stop the job {} because job is already stopped.", jobId);
      }
    }

    // clean up gcs files
    if (job != null) {
      Map<String, String> labelsMap = job.getLabelsMap();
      String basePath = getBasePath(labelsMap.get("namespace"), labelsMap.get("application"),
                                    labelsMap.get("version"), labelsMap.get("program"), labelsMap.get("run"));
      try {
        // delete dir for program run
        gcsStorage.delete(basePath);
      } catch (StorageException e) {
        LOG.warn("GCS path {} is not deleted. ", basePath);
      }
    }
  }

  @Override
  public void kill(RuntimeJobId runtimeJobId) throws Exception {
    Optional<RuntimeJobDetail> jobDetail = getDetail(runtimeJobId);
    if (!jobDetail.isPresent()) {
      return;
    }
    String jobId = runtimeJobId.getRuntimeJobId();
    Job job = null;
    try {
      job = jobControllerClient.cancelJob(projectId, region, jobId);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.FAILED_PRECONDITION) {
        LOG.warn("Failed to stop the job {} because job is already stopped.", jobId);
      }
    }

    // clean up gcs files
    if (job != null) {
      Map<String, String> labelsMap = job.getLabelsMap();
      String basePath = getBasePath(labelsMap.get("namespace"), labelsMap.get("application"),
                                    labelsMap.get("version"), labelsMap.get("program"), labelsMap.get("run"));
      try {
        // delete dir for program run
        gcsStorage.delete(basePath);
      } catch (StorageException e) {
        LOG.warn("GCS path {} is not deleted. ", basePath);
      }
    }
  }

  @Override
  public void close() {
    // close all the resources
  }

  private void uploadFile(Storage storage, String basePath, String fileName,
                          LocalFile localFile) throws IOException, StorageException {
    LOG.debug("Uploading launcher file: {} to gcs bucket {}.", localFile.getName(), bucket);

    try (InputStream inputStream = new FileInputStream(new File(localFile.getURI()))) {
      BlobId blobId = BlobId.of(bucket, basePath + "/" + fileName);
      BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
      try (WriteChannel writer = storage.writer(blobInfo)) {
        ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
      }
    }
  }

  private RuntimeJobStatus getRuntimeJobStatus(Job job) {
    RuntimeJobStatus runtimeJobStatus;

    switch (job.getStatus().getState()) {
      case STATE_UNSPECIFIED:
      case SETUP_DONE:
      case PENDING:
        runtimeJobStatus = RuntimeJobStatus.STARTING;
        break;
      case RUNNING:
        runtimeJobStatus = RuntimeJobStatus.RUNNING;
        break;
      case DONE:
        runtimeJobStatus = RuntimeJobStatus.COMPLETED;
        break;
      case CANCEL_PENDING:
      case CANCEL_STARTED:
        runtimeJobStatus = RuntimeJobStatus.STOPPING;
        break;
      case CANCELLED:
        runtimeJobStatus = RuntimeJobStatus.STOPPED;
        break;
      case ERROR:
        runtimeJobStatus = RuntimeJobStatus.FAILED;
        break;
      default:
        // this needed for ATTEMPT_FAILURE state which is a state for restartable job. Currently we do not launch
        // restartable jobs
        throw new IllegalStateException(String.format("Unsupported job state %s of the dataproc job %s on cluster %s.",
                                                      job.getStatus().getState(), job.getJobUuid(),
                                                      job.getPlacement().getClusterName()));

    }
    return runtimeJobStatus;
  }

  private String getBasePath(String namespace, String application, String version, String program, String run) {
    return Joiner.on("/").join(ImmutableList.of(bucket, namespace, application, version, program, run));
  }
}
