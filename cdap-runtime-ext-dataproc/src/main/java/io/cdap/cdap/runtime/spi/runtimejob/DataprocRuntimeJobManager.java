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

package io.cdap.cdap.runtime.spi.runtimejob;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.dataproc.v1.GetJobRequest;
import com.google.cloud.dataproc.v1.HadoopJob;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.JobControllerSettings;
import com.google.cloud.dataproc.v1.JobPlacement;
import com.google.cloud.dataproc.v1.JobReference;
import com.google.cloud.dataproc.v1.JobStatus;
import com.google.cloud.dataproc.v1.ListJobsRequest;
import com.google.cloud.dataproc.v1.SubmitJobRequest;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Dataproc runtime job manager. This class is responsible for launching a hadoop job on dataproc cluster and
 * managing it. An instance of this class is created by {@code DataprocProvisioner}.
 */
public class DataprocRuntimeJobManager implements RuntimeJobManager {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocRuntimeJobManager.class);

  private static final String CDAP_GCS_ROOT = "cdap-job";
  // dataproc job properties
  private static final String CDAP_RUNTIME_NAMESPACE = "cdap.runtime.namespace";
  private static final String CDAP_RUNTIME_APPLICATION = "cdap.runtime.application";
  private static final String CDAP_RUNTIME_PROGRAM = "cdap.runtime.program";
  private static final String CDAP_RUNTIME_PROGRAM_TYPE = "cdap.runtime.program.type";
  private static final String CDAP_RUNTIME_RUNID = "cdap.runtime.runid";

  private final String clusterName;
  private final GoogleCredentials credentials;
  private final String endpoint;
  private final String projectId;
  private final String region;
  private final String bucket;
  private final Map<String, String> labels;
  private final String sparkCompat;

  private Storage storageClient;
  private JobControllerClient jobControllerClient;
  private String runRootPath;

  /**
   * Created by dataproc provisioner with properties that are needed by dataproc runtime job manager.
   *
   * @param clusterInfo dataproc cluster information
   */
  public DataprocRuntimeJobManager(DataprocClusterInfo clusterInfo) {
    this.clusterName = clusterInfo.getClusterName();
    this.credentials = clusterInfo.getCredentials();
    this.endpoint = clusterInfo.getEndpoint();
    this.projectId = clusterInfo.getProjectId();
    this.region = clusterInfo.getRegion();
    this.bucket = clusterInfo.getBucket();
    this.labels = clusterInfo.getLabels();
    this.sparkCompat = clusterInfo.getSparkCompat();
  }

  @Override
  public void initialize() throws Exception {
    // instantiate a gcs client
    this.storageClient = StorageOptions.newBuilder().setProjectId(projectId)
      .setCredentials(credentials).build().getService();

    // instantiate a dataproc job controller client
    CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
    this.jobControllerClient = JobControllerClient.create(
      JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
        .setEndpoint(region + endpoint).build());
  }

  @Override
  public RuntimeJobId launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    LOG.info("Launching run {} with following configurations: cluster {}, project {}, region {}, bucket {}.",
             runInfo.getRun(), clusterName, projectId, region, bucket);

    // TODO: CDAP-16408 use fixed directory for caching twill, application, artifact jars
    File tempDir = Files.createTempDirectory("dataproc.launcher").toFile();
    // on dataproc bucket the run root will be <bucket>/cdap-job/<runid>/. All the files for this run will be copied
    // under that base dir.
    this.runRootPath = getPath(CDAP_GCS_ROOT, runInfo.getRun());
    try {
      // step 1: build twill.jar and launcher.jar and add them to files to be copied to gcs
      List<RuntimeLocalFile> localFiles = getRuntimeLocalFiles(runtimeJobInfo.getLocalizeFiles(), tempDir);

      // step 2: upload all the necessary files to gcs so that those files are available to dataproc job
      for (RuntimeLocalFile fileToUpload : localFiles) {
        String targetFilePath = getPath(runRootPath, fileToUpload.getName());
        LOG.debug("Uploading file {} to gcs bucket {}.", targetFilePath, bucket);
        uploadFile(targetFilePath, fileToUpload);
        LOG.debug("Uploaded file {} to gcs bucket {}.", targetFilePath, bucket);
      }

      // step 3: build the hadoop job request to be submitted to dataproc
      SubmitJobRequest request = getSubmitJobRequest(runtimeJobInfo.getRuntimeJobClass().getName(),
                                                     runInfo, localFiles);

      // step 4: submit hadoop job to dataproc
      LOG.info("Submitting hadoop job {} to cluster {}.", request.getJob().getReference().getJobId(), clusterName);
      Job job = jobControllerClient.submitJob(request);
      LOG.info("Successfully submitted hadoop job {} to cluster {}.", job.getReference().getJobId(), clusterName);

      return new RuntimeJobId(job.getReference().getJobId());
    } catch (Exception e) {
      // delete all uploaded gcs files in case of exception
      deleteGCSPath(runRootPath);
      throw new Exception(String.format("Error while launching job %s on cluster %s",
                                        runInfo.getRun(), clusterName), e);
    } finally {
      // delete local temp directory
      deleteDirectoryContents(tempDir);
    }
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(RuntimeJobId runtimeJobId) throws Exception {
    String jobId = runtimeJobId.getRuntimeJobId();

    try {
      LOG.info("Getting job details for {} under project {}, region {}.", projectId, region, jobId);
      Job job = jobControllerClient.getJob(GetJobRequest.newBuilder()
                                             .setProjectId(projectId)
                                             .setRegion(region)
                                             .setJobId(jobId)
                                             .build());
      RuntimeJobStatus runtimeJobStatus = getRuntimeJobStatus(job);

      return Optional.of(new RuntimeJobDetail(runtimeJobId, runtimeJobStatus));
    } catch (ApiException e) {
      // this may happen if job is manually deleted by user
      if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
        LOG.warn("Dataproc job {} does not exist in project {}, region {}.", jobId, projectId, region);
      } else {
        throw new Exception(String.format("Error while getting details for job %s on cluster %s.",
                                          jobId, clusterName), e);
      }
    }
    return Optional.empty();
  }

  @Override
  public List<RuntimeJobDetail> list() throws Exception {
    Set<String> filters = new HashSet<>();
    // Dataproc jobs can be filtered by status.state filter. In this case we only want ACTIVE jobs.
    filters.add("status.state=ACTIVE");
    // Filter by labels that were added to the job when this runtime job manager submitted dataproc job. Note that
    // dataproc only supports AND filter.
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      filters.add("labels." + entry.getKey() + "=" + entry.getValue());
    }
    String jobFilter = Joiner.on(" AND ").join(filters);

    LOG.info("Getting a list of jobs under project {}, region {}, cluster {} with filter {}.", projectId, region,
             clusterName, jobFilter);
    JobControllerClient.ListJobsPagedResponse listJobsPagedResponse =
      jobControllerClient.listJobs(ListJobsRequest.newBuilder()
                                     .setProjectId(projectId).setRegion(region).setClusterName(clusterName)
                                     .setFilter(jobFilter).build());

    List<RuntimeJobDetail> jobsDetail = new ArrayList<>();
    for (Job job : listJobsPagedResponse.iterateAll()) {
      jobsDetail.add(new RuntimeJobDetail(new RuntimeJobId(job.getReference().getJobId()), getRuntimeJobStatus(job)));
    }
    return jobsDetail;
  }

  @Override
  public void stop(RuntimeJobId runtimeJobId) throws Exception {
    Optional<RuntimeJobDetail> jobDetail = getDetail(runtimeJobId);
    // if the job does not exist, it can be safely assume that job has been deleted. Hence has reached terminal state.
    if (!jobDetail.isPresent()) {
      return;
    }
    // stop dataproc job
    stopJob(runtimeJobId);
  }

  @Override
  public void kill(RuntimeJobId runtimeJobId) throws Exception {
    stop(runtimeJobId);
  }

  @Override
  public void destroy() {
    jobControllerClient.close();

    if (!Strings.isNullOrEmpty(runRootPath)) {
      deleteGCSPath(runRootPath);
    }
  }

  /**
   * Returns list of runtime local files with twill.jar and launcher.jar added to it.
   */
  private List<RuntimeLocalFile> getRuntimeLocalFiles(Collection<? extends RuntimeLocalFile> runtimeLocalFiles,
                                                      File tempDir) throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(tempDir);
    List<RuntimeLocalFile> localFiles = new ArrayList<>(runtimeLocalFiles);
    localFiles.add(DataprocJarUtil.getTwillJar(locationFactory));
    localFiles.add(DataprocJarUtil.getLauncherJar(locationFactory));
    return localFiles;
  }

  /**
   * Uploads files to gcs.
   */
  private void uploadFile(String path, RuntimeLocalFile localFile) throws IOException, StorageException {
    File file = new File(localFile.getFileUri());
    BlobId blobId = BlobId.of(bucket, path);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();
    try (WriteChannel writer = storageClient.writer(blobInfo)) {
      Files.copy(file.toPath(), Channels.newOutputStream(writer));
    }
  }

  /**
   * Creates and returns dataproc job submit request.
   */
  private SubmitJobRequest getSubmitJobRequest(String jobMainClassName,
                                               ProgramRunInfo runInfo, List<RuntimeLocalFile> localFiles) {
    String runId = runInfo.getRun();
    HadoopJob.Builder hadoopJobBuilder = HadoopJob.newBuilder()
      // set main class
      .setMainClass(DataprocJobMain.class.getName())
      // set main class arguments
      .addAllArgs(ImmutableList.of(jobMainClassName, sparkCompat))
      .putAllProperties(ImmutableMap.of(CDAP_RUNTIME_NAMESPACE, runInfo.getNamespace(),
                                        CDAP_RUNTIME_APPLICATION, runInfo.getApplication(),
                                        CDAP_RUNTIME_PROGRAM, runInfo.getProgram(),
                                        CDAP_RUNTIME_PROGRAM_TYPE, runInfo.getProgramType(),
                                        CDAP_RUNTIME_RUNID, runId));

    for (RuntimeLocalFile localFile : localFiles) {
      String localFileName = localFile.getName();
      String fileName = getPath("gs:/", bucket, CDAP_GCS_ROOT, runId, localFileName);

      // add archive file
      if (localFile.isArchive()) {
        LOG.debug("Adding {} as archive.", localFileName);
        hadoopJobBuilder.addArchiveUris(fileName);
      }

      // add jar file
      if (localFile.getName().endsWith("jar")) {
        LOG.info("Adding {} as jar.", localFileName);
        hadoopJobBuilder.addJarFileUris(fileName);
      } else {
        // add all the other files as file
        LOG.info("Adding {} as file.", localFileName);
        hadoopJobBuilder.addFileUris(fileName);
      }
    }

    return SubmitJobRequest.newBuilder()
      .setRegion(region)
      .setProjectId(projectId)
      .setJob(Job.newBuilder()
                // use program run uuid as hadoop job id on dataproc
                .setReference(JobReference.newBuilder().setJobId(runInfo.getRun()))
                // place the job on provisioned cluster
                .setPlacement(JobPlacement.newBuilder().setClusterName(clusterName).build())
                // add same labels as provisioned cluster
                .putAllLabels(labels)
                .setHadoopJob(hadoopJobBuilder.build())
                .build())
      .build();
  }

  /**
   * Returns {@link RuntimeJobStatus}.
   */
  private RuntimeJobStatus getRuntimeJobStatus(Job job) {
    JobStatus.State state = job.getStatus().getState();
    LOG.debug("Dataproc job {} is in state {}.", job.getReference().getJobId(), state);

    RuntimeJobStatus runtimeJobStatus;
    switch (state) {
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
                                                      job.getStatus().getState(), job.getReference().getJobId(),
                                                      job.getPlacement().getClusterName()));

    }
    return runtimeJobStatus;
  }

  /**
   * Stops the dataproc job. Returns job object if it was stopped.
   */
  private void stopJob(RuntimeJobId runtimeJobId) throws Exception {
    String jobId = runtimeJobId.getRuntimeJobId();
    try {
      jobControllerClient.cancelJob(projectId, region, jobId);
      LOG.info("Stopped the job {} on cluster {}.", jobId, clusterName);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == StatusCode.Code.FAILED_PRECONDITION) {
        LOG.warn("Failed to stop the job {} because job is already stopped on cluster {}.", jobId, clusterName);
      } else {
        throw new Exception(String.format("Error occurred while stopping job %s on cluster %s.",
                                          jobId, clusterName), e);
      }
    }
  }

  private void deleteGCSPath(String path) {
    try {
      removeDir(path);
    } catch (Exception e) {
      LOG.warn(String.format("GCS path %s was not cleaned up for bucket %s due to %s. ",
                             path, bucket, e.getMessage()), e);
    }
  }

  private void removeDir(String path) {
    StorageBatch batch = storageClient.batch();
    Page<Blob> blobs = storageClient.list(bucket, Storage.BlobListOption.currentDirectory(),
                                          Storage.BlobListOption.prefix(path + "/"));
    boolean addedToDelete = false;
    for (Blob blob : blobs.iterateAll()) {
      LOG.debug("Added path to be deleted {}", blob.getName());
      batch.delete(blob.getBlobId());
      addedToDelete = true;
    }

    if (addedToDelete) {
      batch.submit();
    }
  }

  private String getPath(String... pathSubComponents) {
    return Joiner.on("/").join(pathSubComponents);
  }

  /**
   * Recursively deletes all the contents of the directory and the directory itself.
   */
  private static void deleteDirectoryContents(File file) {
    if (file.isDirectory()) {
      File[] entries = file.listFiles();
      if (entries != null) {
        for (File entry : entries) {
          deleteDirectoryContents(entry);
        }
      }
    }
    if (!file.delete()) {
      LOG.warn("Failed to delete temp file {}.", file);
    }
  }
}
