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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.DefaultLocalFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Dataproc runtime job manager. This class is responsible for launching a hadoop job on dataproc cluster and
 * managing it. An instance of this class is created by {@code DataprocProvisioner}.
 */
public class DataprocRuntimeJobManager implements RuntimeJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocRuntimeJobManager.class);

  // dataproc job properties
  private static final String CDAP_RUNTIME_NAMESPACE = "cdap.runtime.namespace";
  private static final String CDAP_RUNTIME_APPLICATION = "cdap.runtime.application";
  private static final String CDAP_RUNTIME_VERSION = "cdap.runtime.version";
  private static final String CDAP_RUNTIME_PROGRAM_TYPE = "cdap.runtime.program.type";
  private static final String CDAP_RUNTIME_PROGRAM = "cdap.runtime.program";
  private static final String CDAP_RUNTIME_RUNID = "cdap.runtime.runid";
  private static final Pattern DATAPROC_JOB_ID_PATTERN = Pattern.compile("[a-zA-Z0-9_-]{0,100}$");

  private final ProvisionerContext provisionerContext;
  private final String clusterName;
  private final GoogleCredentials credentials;
  private final String endpoint;
  private final String projectId;
  private final String region;
  private final String bucket;
  private final Map<String, String> labels;

  private volatile Storage storageClient;
  private volatile JobControllerClient jobControllerClient;

  /**
   * Created by dataproc provisioner with properties that are needed by dataproc runtime job manager.
   *
   * @param clusterInfo dataproc cluster information
   */
  public DataprocRuntimeJobManager(DataprocClusterInfo clusterInfo) {
    this.provisionerContext = clusterInfo.getProvisionerContext();
    this.clusterName = clusterInfo.getClusterName();
    this.credentials = clusterInfo.getCredentials();
    this.endpoint = clusterInfo.getEndpoint();
    this.projectId = clusterInfo.getProjectId();
    this.region = clusterInfo.getRegion();
    this.bucket = clusterInfo.getBucket();
    this.labels = clusterInfo.getLabels();
  }

  /**
   * Returns a {@link Storage} object for interacting with GCS.
   */
  private Storage getStorageClient() {
    Storage client = storageClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = storageClient;
      if (client != null) {
        return client;
      }

      // instantiate a gcs client
      this.storageClient = client = StorageOptions.newBuilder().setProjectId(projectId)
        .setCredentials(credentials).build().getService();
    }
    return client;
  }

  /**
   * Returns a {@link JobControllerClient} to interact with Dataproc Job API.
   */
  private JobControllerClient getJobControllerClient() throws IOException {
    JobControllerClient client = jobControllerClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = jobControllerClient;
      if (client != null) {
        return client;
      }

      // instantiate a dataproc job controller client
      CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
      this.jobControllerClient = client = JobControllerClient.create(
        JobControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
          .setEndpoint(region + endpoint).build());
    }
    return client;
  }

  @Override
  public void initialize() throws Exception {
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    String bucket = DataprocUtils.getBucketName(this.bucket);

    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    LOG.debug("Launching run {} with following configurations: cluster {}, project {}, region {}, bucket {}.",
              runInfo.getRun(), clusterName, projectId, region, bucket);

    // TODO: CDAP-16408 use fixed directory for caching twill, application, artifact jars
    File tempDir = Files.createTempDirectory("dataproc.launcher").toFile();
    // on dataproc bucket the run root will be <bucket>/cdap-job/<runid>/. All the files for this run will be copied
    // under that base dir.
    String runRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT, runInfo.getRun());
    try {
      // step 1: build twill.jar and launcher.jar and add them to files to be copied to gcs
      List<LocalFile> localFiles = getRuntimeLocalFiles(runtimeJobInfo.getLocalizeFiles(), tempDir);

      // step 2: upload all the necessary files to gcs so that those files are available to dataproc job
      List<LocalFile> uploadedFiles = new ArrayList<>();
      for (LocalFile fileToUpload : localFiles) {
        String targetFilePath = getPath(runRootPath, fileToUpload.getName());
        uploadedFiles.add(uploadFile(bucket, targetFilePath, fileToUpload));
        LOG.debug("Uploaded file from {} to gs://{}/{}.", fileToUpload.getURI(), bucket, targetFilePath);
      }

      // step 3: build the hadoop job request to be submitted to dataproc
      SubmitJobRequest request = getSubmitJobRequest(runtimeJobInfo.getRuntimeJobClassname(), runInfo, uploadedFiles);

      // step 4: submit hadoop job to dataproc
      Job job = getJobControllerClient().submitJob(request);
      LOG.debug("Successfully submitted hadoop job {} to cluster {}.", job.getReference().getJobId(), clusterName);
    } catch (Exception e) {
      // delete all uploaded gcs files in case of exception
      DataprocUtils.deleteGCSPath(getStorageClient(), bucket, runRootPath);
      throw new Exception(String.format("Error while launching job %s on cluster %s",
                                        getJobId(runInfo), clusterName), e);
    } finally {
      // delete local temp directory
      deleteDirectoryContents(tempDir);
    }
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) throws Exception {
    String jobId = getJobId(programRunInfo);

    try {
      Job job = getJobControllerClient().getJob(GetJobRequest.newBuilder()
                                                  .setProjectId(projectId)
                                                  .setRegion(region)
                                                  .setJobId(jobId)
                                                  .build());
      return Optional.of(new RuntimeJobDetail(getProgramRunInfo(job), getRuntimeJobStatus(job)));
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() != StatusCode.Code.NOT_FOUND) {
        throw new Exception(String.format("Error while getting details for job %s on cluster %s.",
                                          jobId, clusterName), e);
      }
      // Status is not found if job is finished or manually deleted by the user
      LOG.debug("Dataproc job {} does not exist in project {}, region {}.", jobId, projectId, region);
    }
    return Optional.empty();
  }

  @Override
  public List<RuntimeJobDetail> list() throws IOException {
    Set<String> filters = new HashSet<>();
    // Dataproc jobs can be filtered by status.state filter. In this case we only want ACTIVE jobs.
    filters.add("status.state=ACTIVE");
    // Filter by labels that were added to the job when this runtime job manager submitted dataproc job. Note that
    // dataproc only supports AND filter.
    for (Map.Entry<String, String> entry : labels.entrySet()) {
      filters.add("labels." + entry.getKey() + "=" + entry.getValue());
    }
    String jobFilter = Joiner.on(" AND ").join(filters);

    LOG.debug("Getting a list of jobs under project {}, region {}, cluster {} with filter {}.", projectId, region,
             clusterName, jobFilter);
    JobControllerClient.ListJobsPagedResponse listJobsPagedResponse =
      getJobControllerClient().listJobs(
        ListJobsRequest.newBuilder()
          .setProjectId(projectId).setRegion(region).setClusterName(clusterName)
          .setFilter(jobFilter).build());

    List<RuntimeJobDetail> jobsDetail = new ArrayList<>();
    for (Job job : listJobsPagedResponse.iterateAll()) {
      jobsDetail.add(new RuntimeJobDetail(getProgramRunInfo(job), getRuntimeJobStatus(job)));
    }
    return jobsDetail;
  }

  @Override
  public void stop(ProgramRunInfo programRunInfo) throws Exception {
    RuntimeJobDetail jobDetail = getDetail(programRunInfo).orElse(null);
    if (jobDetail == null) {
      return;
    }
    RuntimeJobStatus status = jobDetail.getStatus();
    if (status.isTerminated() || status == RuntimeJobStatus.STOPPING) {
      return;
    }
    // stop dataproc job
    stopJob(getJobId(jobDetail.getRunInfo()));
  }

  @Override
  public void kill(ProgramRunInfo programRunInfo) throws Exception {
    stop(programRunInfo);
  }

  @Override
  public void close() {
    JobControllerClient client = this.jobControllerClient;
    if (client != null) {
      client.close();
    }
  }

  /**
   * Returns list of runtime local files with twill.jar and launcher.jar added to it.
   */
  private List<LocalFile> getRuntimeLocalFiles(Collection<? extends LocalFile> runtimeLocalFiles,
                                               File tempDir) throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(tempDir);
    List<LocalFile> localFiles = new ArrayList<>(runtimeLocalFiles);
    localFiles.add(DataprocJarUtil.getTwillJar(locationFactory));
    localFiles.add(DataprocJarUtil.getLauncherJar(locationFactory));
    return localFiles;
  }

  /**
   * Uploads files to gcs.
   */
  private LocalFile uploadFile(String bucket, String targetFilePath,
                               LocalFile localFile) throws IOException, StorageException {
    BlobId blobId = BlobId.of(bucket, targetFilePath);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("application/octet-stream").build();

    try (InputStream inputStream = openStream(localFile.getURI());
         WriteChannel writer = getStorageClient().writer(blobInfo)) {
      ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
    }

    return new DefaultLocalFile(localFile.getName(), URI.create(String.format("gs://%s/%s", bucket, targetFilePath)),
                                localFile.getLastModified(), localFile.getSize(),
                                localFile.isArchive(), localFile.getPattern());
  }

  /**
   * Opens an {@link InputStream} to read from the given URI.
   */
  private InputStream openStream(URI uri) throws IOException {
    if ("file".equals(uri.getScheme())) {
      return Files.newInputStream(new File(uri).toPath());
    }
    LocationFactory locationFactory = provisionerContext.getLocationFactory();
    if (locationFactory.getHomeLocation().toURI().getScheme().equals(uri.getScheme())) {
      return locationFactory.create(uri).getInputStream();
    }
    if ("gs".equals(uri.getScheme())) {
      BlobId blobId = BlobId.of(uri.getAuthority(), uri.getPath().substring(1));
      Storage client = StorageOptions.getDefaultInstance().getService();
      return Channels.newInputStream(client.get(blobId).reader());
    }

    // Default to Java URL stream implementation.
    return uri.toURL().openStream();
  }

  /**
   * Creates and returns dataproc job submit request.
   */
  private SubmitJobRequest getSubmitJobRequest(String jobMainClassName,
                                               ProgramRunInfo runInfo, List<LocalFile> localFiles) {
    String runId = runInfo.getRun();

    // The DataprocJobMain argument is <class-name> <spark-compat> <list of archive files...>
    List<String> arguments = Stream.concat(
      Stream.of(jobMainClassName, provisionerContext.getSparkCompat().getCompat()),
      localFiles.stream().filter(LocalFile::isArchive).map(LocalFile::getName)
    ).collect(Collectors.toList());

    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(CDAP_RUNTIME_NAMESPACE, runInfo.getNamespace());
    properties.put(CDAP_RUNTIME_APPLICATION, runInfo.getApplication());
    properties.put(CDAP_RUNTIME_VERSION, runInfo.getVersion());
    properties.put(CDAP_RUNTIME_PROGRAM, runInfo.getProgram());
    properties.put(CDAP_RUNTIME_PROGRAM_TYPE, runInfo.getProgramType());
    properties.put(CDAP_RUNTIME_RUNID, runId);

    HadoopJob.Builder hadoopJobBuilder = HadoopJob.newBuilder()
      // set main class
      .setMainClass(DataprocJobMain.class.getName())
      // set main class arguments
      .addAllArgs(arguments)
      .putAllProperties(properties);

    for (LocalFile localFile : localFiles) {
      // add jar file
      URI uri = localFile.getURI();
      if (localFile.getName().endsWith("jar")) {
        hadoopJobBuilder.addJarFileUris(uri.toString());
      } else {
        hadoopJobBuilder.addFileUris(uri.toString());
      }
    }

    return SubmitJobRequest.newBuilder()
      .setRegion(region)
      .setProjectId(projectId)
      .setJob(Job.newBuilder()
                // use program run uuid as hadoop job id on dataproc
                .setReference(JobReference.newBuilder().setJobId(getJobId(runInfo)))
                // place the job on provisioned cluster
                .setPlacement(JobPlacement.newBuilder().setClusterName(clusterName).build())
                // add same labels as provisioned cluster
                .putAllLabels(labels)
                .setHadoopJob(hadoopJobBuilder.build())
                .build())
      .build();
  }

  private ProgramRunInfo getProgramRunInfo(Job job) {
    Map<String, String> jobProperties = job.getHadoopJob().getPropertiesMap();

    ProgramRunInfo.Builder builder = new ProgramRunInfo.Builder()
      .setNamespace(jobProperties.get(CDAP_RUNTIME_NAMESPACE))
      .setApplication(jobProperties.get(CDAP_RUNTIME_APPLICATION))
      .setVersion(jobProperties.get(CDAP_RUNTIME_VERSION))
      .setProgramType(jobProperties.get(CDAP_RUNTIME_PROGRAM_TYPE))
      .setProgram(jobProperties.get(CDAP_RUNTIME_PROGRAM))
      .setRun(jobProperties.get(CDAP_RUNTIME_RUNID));
    return builder.build();
  }

  /**
   * Returns {@link RuntimeJobStatus}.
   */
  private RuntimeJobStatus getRuntimeJobStatus(Job job) {
    JobStatus.State state = job.getStatus().getState();
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
  private void stopJob(String jobId) throws Exception {
    try {
      jobControllerClient.cancelJob(projectId, region, jobId);
      LOG.debug("Stopped the job {} on cluster {}.", jobId, clusterName);
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() != StatusCode.Code.FAILED_PRECONDITION) {
        throw new Exception(String.format("Error occurred while stopping job %s on cluster %s.",
                                          jobId, clusterName), e);
      }
      LOG.debug("Job {} is already stopped on cluster {}.", jobId, clusterName);
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

  /**
   * Returns job name from run info.
   * namespace, application, program, run(36 characters)
   * Example: namespace_application_program_8e1cb2ce-a102-48cf-a959-c4f991a2b475
   *
   * The ID must contain only letters (a-z, A-Z), numbers (0-9), underscores (_), or hyphens (-).
   * The maximum length is 100 characters.
   *
   * @throws IllegalArgumentException if provided id does not comply with naming restrictions
   */
  @VisibleForTesting
  public static String getJobId(ProgramRunInfo runInfo) {
    List<String> parts = ImmutableList.of(runInfo.getNamespace(), runInfo.getApplication(), runInfo.getProgram());
    String joined = Joiner.on("_").join(parts);
    joined = joined.substring(0, Math.min(joined.length(), 63));
    joined = joined + "_" + runInfo.getRun();
    if (!DATAPROC_JOB_ID_PATTERN.matcher(joined).matches()) {
      throw new IllegalArgumentException(String.format("Job ID %s is not a valid dataproc job id. ", joined));
    }

    return joined;
  }
}
