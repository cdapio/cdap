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
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.WriteChannel;
import com.google.cloud.dataproc.v1beta2.GetJobRequest;
import com.google.cloud.dataproc.v1beta2.HadoopJob;
import com.google.cloud.dataproc.v1beta2.Job;
import com.google.cloud.dataproc.v1beta2.JobControllerClient;
import com.google.cloud.dataproc.v1beta2.JobControllerSettings;
import com.google.cloud.dataproc.v1beta2.JobPlacement;
import com.google.cloud.dataproc.v1beta2.JobReference;
import com.google.cloud.dataproc.v1beta2.JobStatus;
import com.google.cloud.dataproc.v1beta2.SubmitJobRequest;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.StorageRetryStrategy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.error.api.ErrorTagProvider;
import io.cdap.cdap.runtime.spi.CacheableLocalFile;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.VersionInfo;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.dataproc.DataprocRuntimeException;
import joptsimple.internal.Strings;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Dataproc runtime job manager. This class is responsible for launching a hadoop job on dataproc cluster and managing
 * it. An instance of this class is created by {@code DataprocProvisioner}.
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

  //dataproc job labels (must match '[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62}' pattern)
  private static final String LABEL_CDAP_PROGRAM = "cdap-program";
  private static final String LABEL_CDAP_PROGRAM_TYPE = "cdap-program-type";

  // Dataproc specific error groups
  private static final String ERRGP_GCS = "gcs";

  private final ProvisionerContext provisionerContext;
  private final String clusterName;
  private final GoogleCredentials credentials;
  private final String endpoint;
  private final String projectId;
  private final String region;
  private final String bucket;
  private final Map<String, String> labels;
  private final Map<String, String> provisionerProperties;
  private final VersionInfo cdapVersionInfo;

  private volatile Storage storageClient;
  private volatile JobControllerClient jobControllerClient;
  List<String> commonArtifactsPerCDAPVersion = new ArrayList<>(
    Arrays.asList(Constants.Files.TWILL_JAR, Constants.Files.LAUNCHER_JAR, Constants.Files.APPLICATION_JAR)
  );

  /**
   * Created by dataproc provisioner with properties that are needed by dataproc runtime job manager.
   *
   * @param clusterInfo dataproc cluster information
   */
  public DataprocRuntimeJobManager(DataprocClusterInfo clusterInfo,
                                   Map<String, String> provisionerProperties, VersionInfo cdapVersionInfo) {
    this.provisionerContext = clusterInfo.getProvisionerContext();
    this.clusterName = clusterInfo.getClusterName();
    this.credentials = clusterInfo.getCredentials();
    this.endpoint = clusterInfo.getEndpoint();
    this.projectId = clusterInfo.getProjectId();
    this.region = clusterInfo.getRegion();
    this.bucket = clusterInfo.getBucket();
    this.labels = clusterInfo.getLabels();
    // Provisioner properties contains overrides for properties defined in cdap-site.
    // These properties are absent in provisionerContext.getProperties().
    this.provisionerProperties = provisionerProperties;
    this.cdapVersionInfo = cdapVersionInfo;
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
      this.storageClient = client = StorageOptions.newBuilder()
        .setStorageRetryStrategy(StorageRetryStrategy.getDefaultStorageRetryStrategy())
        .setProjectId(projectId)
        .setCredentials(credentials)
        .build()
        .getService();
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
          .setEndpoint(region + "-" + endpoint).build());
    }
    return client;
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    String bucket = DataprocUtils.getBucketName(this.bucket);
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();

    // Caching is disabled if it's been explicitly disabled or delete lifecycle is not set on the bucket.
    boolean disableGCSCaching = Boolean.parseBoolean(
      provisionerContext.getProperties().getOrDefault(DataprocUtils.DISABLE_GCS_CACHING, "false"))
      || !isDeleteLifecycleEnabled(bucket);

    LOG.debug("Launching run {} with following configurations: cluster {}, project {}, region {}, bucket {}.",
              runInfo.getRun(), clusterName, projectId, region, bucket);
    if (disableGCSCaching) {
      LOG.warn("Launching run {} without GCS caching. This slows launch time.", runInfo.getRun());
    }

    File tempDir = DataprocUtils.CACHE_DIR_PATH.toFile();
    boolean disableLocalCaching = Boolean.parseBoolean(
      provisionerContext.getProperties().getOrDefault(DataprocUtils.LOCAL_CACHE_DISABLED,
                                                      "false"));
    // In dataproc bucket, the run root will be <bucket>/cdap-job/<runid>/. All the files without _cache_ in their
    // filename for this run will be copied under that base dir.
    String runRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT, runInfo.getRun());
    // In dataproc bucket, the shared folder for artifacts will be <bucket>/cdap-job/cached-artifacts.
    // All instances of CacheableLocalFile will be copied to the shared folder if they do not exist.
    String cacheRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT, DataprocUtils.CDAP_CACHED_ARTIFACTS);
    boolean enableGCSCachingSnapshot = Boolean.parseBoolean(
      provisionerContext.getProperties().getOrDefault(DataprocUtils.ENABLE_GCS_CACHING_SNAPSHOT, "false"));
    // cdap_version is of the form "major.minor.fix-buildTime"/"major.minor.fix-SNAPSHOT-buildTime"
    String[] appCDAPVersion = cdapVersionInfo.toString().split("-");
    boolean enableCommonArtifactsCaching = !disableGCSCaching &&
      ((appCDAPVersion.length > 1 && !"SNAPSHOT".equals(appCDAPVersion[1])) || enableGCSCachingSnapshot);

    try {
      // step 1: build twill.jar and launcher.jar and add them to files to be copied to gcs
      if (disableLocalCaching) {
        LOG.debug("Local caching is disabled, " +
                    "continuing without caching twill and dataproc launcher jars.");
        tempDir = Files.createTempDirectory("dataproc.launcher").toFile();
      }
      List<LocalFile> localFiles = getRuntimeLocalFiles(runtimeJobInfo.getLocalizeFiles(), tempDir);

      // step 2: upload all the necessary files to gcs so that those files are available to dataproc job
      List<Future<LocalFile>> uploadFutures = new ArrayList<>();
      for (LocalFile fileToUpload : localFiles) {
        boolean isCacheable = !disableGCSCaching && fileToUpload instanceof CacheableLocalFile;
        String targetFilePath = getPath(isCacheable ? cacheRootPath : runRootPath, fileToUpload.getName());
        String targetFilePathWithVersion = getPath(cacheRootPath, appCDAPVersion[0], fileToUpload.getName());

        if (commonArtifactsPerCDAPVersion.contains(fileToUpload.getName()) && enableCommonArtifactsCaching) {
          // upload artifacts common per cdap version to <bucket>/cdap-job/cached-artifacts/<cdapVersion>/
          uploadFutures.add(
            provisionerContext.execute(
              () -> uploadCommonArtifactsPerCDAPVersion(bucket, targetFilePathWithVersion, fileToUpload))
              .toCompletableFuture());
        } else {
          // upload cacheable artifacts to <bucket>/cdap-job/cached-artifacts/ and
          // non-cacheable artifacts to <bucket>/cdap-job/<runid>/
          uploadFutures.add(
            provisionerContext.execute(() -> isCacheable ? uploadCacheableFile(bucket, targetFilePath, fileToUpload) :
                uploadFile(bucket, targetFilePath, fileToUpload, false, false))
              .toCompletableFuture());
        }
      }

      List<LocalFile> uploadedFiles = new ArrayList<>();
      for (Future<LocalFile> uploadFuture : uploadFutures) {
        uploadedFiles.add(uploadFuture.get());
      }

      // step 3: build the hadoop job request to be submitted to dataproc
      SubmitJobRequest request = getSubmitJobRequest(runtimeJobInfo, uploadedFiles);

      // step 4: submit hadoop job to dataproc
      try {
        Job job = getJobControllerClient().submitJob(request);
        LOG.debug("Successfully submitted hadoop job {} to cluster {}.", job.getReference().getJobId(), clusterName);
      } catch (AlreadyExistsException ex) {
        //the job id already exists, ignore the job.
        LOG.warn("The dataproc job {} already exists. Ignoring resubmission of the job.",
                 request.getJob().getReference().getJobId());
      }
      DataprocUtils.emitMetric(provisionerContext, region,
                               "provisioner.submitJob.response.count");
    } catch (Exception e) {
      String errorMessage = String.format("Error while launching job %s on cluster %s.",
        getJobId(runInfo), clusterName);
      // delete all uploaded gcs files in case of exception
      DataprocUtils.deleteGCSPath(getStorageClient(), bucket, runRootPath);
      DataprocUtils.emitMetric(provisionerContext, region,
                               "provisioner.submitJob.response.count", e);
      // ResourceExhaustedException indicates Dataproc agent running on master node isn't emitting heartbeat.
      // This usually indicates master VM crashing due to OOM.
      if (e instanceof ResourceExhaustedException) {
        String message = String.format("%s Cluster can't accept jobs presently: %s",
                                       errorMessage,
                                       Throwables.getRootCause(e).getMessage());
        String helpMessage = DataprocUtils.getTroubleshootingHelpMessage(
            provisionerProperties.getOrDefault(
              DataprocUtils.TROUBLESHOOTING_DOCS_URL_KEY,
              DataprocUtils.TROUBLESHOOTING_DOCS_URL_DEFAULT));

        String combined = Stream.of(message, helpMessage)
          .filter(s -> !Strings.isNullOrEmpty(s))
          .collect(Collectors.joining("\n"));

        throw new DataprocRuntimeException(e, combined, ErrorTagProvider.ErrorTag.USER);
      }
      throw new DataprocRuntimeException(e, errorMessage);
    } finally {
      if (disableLocalCaching) {
        DataprocUtils.deleteDirectoryContents(tempDir);
      }
    }
  }

  private LocalFile uploadCommonArtifactsPerCDAPVersion(String bucket, String targetFilePath, LocalFile localFile)
    throws IOException, InterruptedException {
    Storage storage = getStorageClient();
    BlobId blobId = BlobId.of(bucket, targetFilePath);
    Blob blob = storage.get(blobId);
    LocalFile result;

    if (blob != null && blob.exists()) {
      LOG.debug("Skip uploading file {} to gs://{}/{} because it exists.",
                localFile.getURI(), bucket, targetFilePath);
      result = new DefaultLocalFile(localFile.getName(),
                                    URI.create(String.format("gs://%s/%s", bucket, targetFilePath)),
                                    localFile.getLastModified(), localFile.getSize(),
                                    localFile.isArchive(), localFile.getPattern());
    } else {
      result = uploadFile(bucket, targetFilePath, localFile, false, true);
    }

    return result;
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
  public void stop(ProgramRunInfo programRunInfo) throws Exception {
    RuntimeJobDetail jobDetail = getDetail(programRunInfo).orElse(null);
    kill(jobDetail);
  }

  @Override
  public void kill(RuntimeJobDetail jobDetail) throws Exception {
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
    localFiles.add(getTwillJar(locationFactory));
    localFiles.add(getLauncherJar(locationFactory));

    // Sort files in descending order by size so that we upload concurrently large files first.
    localFiles.sort(Comparator.comparingLong(LocalFile::getSize).reversed());

    return localFiles;
  }

  private LocalFile getTwillJar(LocationFactory locationFactory) throws IOException {
    Location location = locationFactory.create(Constants.Files.TWILL_JAR);
    if (location.exists()) {
      return DataprocJarUtil.getLocalFile(location, true);
    }
    return DataprocJarUtil.getTwillJar(locationFactory);
  }

  private LocalFile getLauncherJar(LocationFactory locationFactory) throws IOException {
    Location location = locationFactory.create(Constants.Files.LAUNCHER_JAR);
    if (location.exists()) {
      return DataprocJarUtil.getLocalFile(location, false);
    }
    return DataprocJarUtil.getLauncherJar(locationFactory);
  }

  /**
   * Check whether delete lifecycle with days since custom time has been enabled on the bucket or not.
   * @param bucketName
   * @return true if delete lifecycle with days since custom time is set on the bucket.
   */
  private boolean isDeleteLifecycleEnabled(String bucketName) {
    Storage storage = getStorageClient();
    Bucket bucket = storage.get(bucketName);
    for (BucketInfo.LifecycleRule rule : bucket.getLifecycleRules()) {
      if (rule.getAction() == null || rule.getCondition() == null ||
        rule.getCondition().getDaysSinceCustomTime() == null) {
        continue;
      }
      if (rule.getAction() instanceof BucketInfo.LifecycleRule.DeleteLifecycleAction &&
        rule.getCondition().getDaysSinceCustomTime() > 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Upload cacheable files uploads the file to GCS if the file does not exists. Once uploaded, it also sets custom time
   * on the object.
   */
  private LocalFile uploadCacheableFile(String bucket, String targetFilePath,
                                        LocalFile localFile)
    throws IOException, StorageException, InterruptedException {
    Storage storage = getStorageClient();
    BlobId blobId = BlobId.of(bucket, targetFilePath);
    Blob blob = storage.get(blobId);
    LocalFile result;

    if (blob != null && blob.exists()) {
      LOG.debug("Skip uploading file {} to gs://{}/{} because it exists.",
                localFile.getURI(), bucket, targetFilePath);
      result = new DefaultLocalFile(localFile.getName(),
                                    URI.create(String.format("gs://%s/%s", bucket, targetFilePath)),
                                    localFile.getLastModified(), localFile.getSize(),
                                    localFile.isArchive(), localFile.getPattern());
    } else {
      result = uploadFile(bucket, targetFilePath, localFile, true, false);
    }

    return result;
  }

  /**
   * Uploads files to gcs.
   */
  private LocalFile uploadFile(String bucket, String targetFilePath,
                               LocalFile localFile, boolean isCacheable, boolean isCacheablePerVersion)
    throws IOException, StorageException, InterruptedException {
    BlobId blobId = BlobId.of(bucket, targetFilePath);
    String contentType = "application/octet-stream";
    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
    if (isCacheable) {
      blobInfoBuilder.setCustomTime(System.currentTimeMillis());
    }
    BlobInfo blobInfo = blobInfoBuilder.setContentType(contentType).build();
    Storage storage = getStorageClient();

    Bucket bucketObj = storage.get(bucket);
    if (bucketObj == null) {
      throw new IOException("GCS bucket '" + bucket + "'does not exists");
    }

    LOG.debug("Uploading a file of size {} bytes from {} to gs://{}/{} of {} bucket type located at {}",
              localFile.getSize(), localFile.getURI(), bucket, targetFilePath,
              bucketObj.getLocationType(), bucketObj.getLocation());
    try {
      long start = System.nanoTime();
      uploadToGCS(localFile.getURI(), storage, blobInfo, Storage.BlobWriteOption.doesNotExist());
      long end = System.nanoTime();
      LOG.debug("Successfully uploaded file {} to gs://{}/{} in {} ms.",
                localFile.getURI(), bucket, targetFilePath, TimeUnit.NANOSECONDS.toMillis(end - start));
    } catch (StorageException e) {
      if (e.getCode() != HttpURLConnection.HTTP_PRECON_FAILED) {
        throw e;
      }

      if (!isCacheable && !isCacheablePerVersion) {
        // Precondition fails means the blob already exists, most likely happens due to retries
        // https://cloud.google.com/storage/docs/request-preconditions#special-case
        // Overwrite the file
        Blob blob = storage.get(blobId);
        BlobInfo existingBlobInfo = BlobInfo.newBuilder(blob.getBlobId()).setContentType(contentType).build();
        uploadToGCS(localFile.getURI(), storage, existingBlobInfo, Storage.BlobWriteOption.generationMatch());
        LOG.debug("Successfully uploaded file {} to gs://{}/{} by overwriting due to conflict",
                  localFile.getURI(), bucket, targetFilePath);
      } else {
        LOG.debug("Skip uploading file {} to gs://{}/{} because it exists.",
                  localFile.getURI(), bucket, targetFilePath);
      }
    }

    return new DefaultLocalFile(localFile.getName(), URI.create(String.format("gs://%s/%s", bucket, targetFilePath)),
                                localFile.getLastModified(), localFile.getSize(),
                                localFile.isArchive(), localFile.getPattern());
  }

  /**
   * Uploads the file to GCS bucket.
   */
  private void uploadToGCS(java.net.URI localFileUri, Storage storage, BlobInfo blobInfo,
                           Storage.BlobWriteOption... blobWriteOptions) throws IOException, StorageException {
    try (InputStream inputStream = openStream(localFileUri);
         WriteChannel writer = storage.writer(blobInfo, blobWriteOptions)) {
      ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
    }
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
  private SubmitJobRequest getSubmitJobRequest(RuntimeJobInfo runtimeJobInfo, List<LocalFile> localFiles) {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    String runId = runInfo.getRun();

    // The DataprocJobMain argument is <class-name> <spark-compat> <list of archive files...>
    List<String> arguments = new ArrayList<>();
    arguments.add("--" + DataprocJobMain.RUNTIME_JOB_CLASS + "=" + runtimeJobInfo.getRuntimeJobClassname());
    arguments.add("--" + DataprocJobMain.SPARK_COMPAT + "=" + provisionerContext.getSparkCompat().getCompat());
    localFiles.stream()
      .filter(LocalFile::isArchive)
      .map(f -> "--" + DataprocJobMain.ARCHIVE + "=" + f.getName())
      .forEach(arguments::add);
    for (Map.Entry<String, String> entry : runtimeJobInfo.getJvmProperties().entrySet()) {
      arguments.add("--" + DataprocJobMain.PROPERTY_PREFIX + entry.getKey() + "=\"" + entry.getValue() + "\"");
    }

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
                // Job label values must match the pattern '[\p{Ll}\p{Lo}\p{N}_-]{0,63}'
                // Since program name and type are class names they should follow that pattern once we remove all
                // capitals
                .putLabels(LABEL_CDAP_PROGRAM, runInfo.getProgram().toLowerCase())
                .putLabels(LABEL_CDAP_PROGRAM_TYPE, runInfo.getProgramType().toLowerCase())
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
   * Returns job name from run info.
   * namespace, application, program, run(36 characters)
   * Example: namespace_application_program_8e1cb2ce-a102-48cf-a959-c4f991a2b475
   * <p>
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
