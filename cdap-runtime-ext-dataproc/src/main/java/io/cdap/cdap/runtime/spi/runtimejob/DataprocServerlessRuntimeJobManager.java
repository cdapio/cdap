/*
 * Copyright © 2026 Cask Data, Inc.
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
import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.BatchControllerClient;
import com.google.cloud.dataproc.v1.BatchControllerSettings;
import com.google.cloud.dataproc.v1.CreateBatchRequest;
import com.google.cloud.dataproc.v1.DeleteBatchRequest;
import com.google.cloud.dataproc.v1.EnvironmentConfig;
import com.google.cloud.dataproc.v1.ExecutionConfig;
import com.google.cloud.dataproc.v1.GetBatchRequest;
import com.google.cloud.dataproc.v1.RuntimeConfig;
import com.google.cloud.dataproc.v1.SparkBatch;
import com.google.cloud.http.HttpTransportOptions;
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
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ErrorUtils.ActionErrorPair;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.runtime.spi.CacheableLocalFile;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.VersionInfo;
import io.cdap.cdap.runtime.spi.common.DataprocMetric;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.dataproc.DataprocRuntimeException;
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
import javax.annotation.Nullable;
import org.apache.twill.api.LocalFile;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.DefaultLocalFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/**
 * Dataproc Serverless runtime job manager.
 */
public class DataprocServerlessRuntimeJobManager implements RuntimeJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocServerlessRuntimeJobManager.class);

  // Dataproc job properties
  public static final String CDAP_RUNTIME_NAMESPACE = "cdap.runtime.namespace";
  public static final String CDAP_RUNTIME_APPLICATION = "cdap.runtime.application";
  public static final String CDAP_RUNTIME_VERSION = "cdap.runtime.version";
  public static final String CDAP_RUNTIME_PROGRAM_TYPE = "cdap.runtime.program.type";
  public static final String CDAP_RUNTIME_PROGRAM = "cdap.runtime.program";
  public static final String CDAP_RUNTIME_RUNID = "cdap.runtime.runid";

  private static final String GCS_DOC_URL =
      "https://cloud.google.com/storage/docs/json_api/v1/status-codes";

  private final ProvisionerContext provisionerContext;
  private final GoogleCredentials credentials;
  private final String endpoint;
  private final String projectId;
  private final String region;
  private final String bucket;
  private final Map<String, String> labels;
  private final Map<String, String> provisionerProperties;
  private final VersionInfo cdapVersionInfo;

  private volatile Storage storageClient;
  private volatile BatchControllerClient batchControllerClient;

  private static final List<String> artifactsCacheablePerCDAPVersion = new ArrayList<>(
      Arrays.asList(Constants.Files.TWILL_JAR, Constants.Files.LAUNCHER_JAR)
  );
  private static final int SNAPSHOT_EXPIRE_DAYS = 7;
  private static final int EXPIRE_DAYS = 730;

  /**
   * Constructs a new DataprocServerlessRuntimeJobManager.
   */
  public DataprocServerlessRuntimeJobManager(DataprocClusterInfo clusterInfo,
      Map<String, String> provisionerProperties, VersionInfo cdapVersionInfo) {
    this.provisionerContext = clusterInfo.getProvisionerContext();
    this.credentials = clusterInfo.getCredentials();
    this.endpoint = clusterInfo.getEndpoint();
    this.projectId = clusterInfo.getProjectId();
    this.region = clusterInfo.getRegion();
    this.bucket = clusterInfo.getBucket();
    this.labels = clusterInfo.getLabels();
    this.provisionerProperties = provisionerProperties;
    this.cdapVersionInfo = cdapVersionInfo;
  }

  /**
   * Retrieves the Storage client, creating it if it doesn't already exist.
   */
  @VisibleForTesting
  public Storage getStorageClient() {
    Storage client = storageClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = storageClient;
      if (client != null) {
        return client;
      }

      int gcsHttpRequestConnectionTimeout = Integer.parseInt(provisionerProperties.getOrDefault(
          DataprocUtils.GCS_HTTP_REQUEST_CONNECTION_TIMEOUT_MILLIS,
          DataprocUtils.GCS_HTTP_REQUEST_CONNECTION_TIMEOUT_MILLIS_DEFAULT
      ));
      int gcsHttpRequestReadTimeout = Integer.parseInt(provisionerProperties.getOrDefault(
          DataprocUtils.GCS_HTTP_REQUEST_READ_TIMEOUT_MILLIS,
          DataprocUtils.GCS_HTTP_REQUEST_READ_TIMEOUT_MILLIS_DEFAULT
      ));
      int gcsHttpRequestTotalTimeout = Integer.parseInt(provisionerProperties.getOrDefault(
          DataprocUtils.GCS_HTTP_REQUEST_TOTAL_TIMEOUT_MINS,
          DataprocUtils.GCS_HTTP_REQUEST_TOTAL_TIMEOUT_MINS_DEFAULT
      ));

      HttpTransportOptions transportOptions = StorageOptions.getDefaultHttpTransportOptions()
          .toBuilder()
          .setConnectTimeout(gcsHttpRequestConnectionTimeout)
          .setReadTimeout(gcsHttpRequestReadTimeout)
          .build();

      this.storageClient = client = StorageOptions.newBuilder()
          .setStorageRetryStrategy(StorageRetryStrategy.getUniformStorageRetryStrategy())
          .setProjectId(projectId)
          .setCredentials(credentials)
          .setRetrySettings(StorageOptions.getDefaultRetrySettings().toBuilder()
              .setTotalTimeout(Duration.ofMinutes(gcsHttpRequestTotalTimeout)).build())
          .setTransportOptions(transportOptions)
          .build()
          .getService();
    }
    return client;
  }

  private BatchControllerClient getBatchControllerClient() throws IOException {
    BatchControllerClient client = batchControllerClient;
    if (client != null) {
      return client;
    }

    synchronized (this) {
      client = batchControllerClient;
      if (client != null) {
        return client;
      }

      CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
      this.batchControllerClient = client = BatchControllerClient.create(
          BatchControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
              .setEndpoint(String.format("%s-%s", region, endpoint)).build());
    }
    return client;
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    String bucket = DataprocUtils.getBucketName(this.bucket);
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    String batchId = getBatchId(runInfo);

    LOG.warn("TEST_LOG: Entering DataprocServerlessRuntimeJobManager.launch. runId: {}, batchId: {}, "
                 + "namespace: {}, app: {}, program: {}",
             runInfo.getRun(), batchId, runInfo.getNamespace(), runInfo.getApplication(), runInfo.getProgram());

    boolean gcsCacheEnabled = Boolean.parseBoolean(
        provisionerContext.getProperties().getOrDefault(DataprocUtils.GCS_CACHE_ENABLED, "true"))
        || !validateDeleteLifecycle(bucket, runInfo.getRun());

    LOG.warn("TEST_LOG: Launch parameters - gcsCacheEnabled: {}, projectId: {}, region: {}, bucket: {}",
             gcsCacheEnabled, projectId, region, bucket);

    File tempDir = DataprocUtils.CACHE_DIR_PATH.toFile();
    boolean disableLocalCaching = Boolean.parseBoolean(
        provisionerContext.getProperties().getOrDefault(DataprocUtils.LOCAL_CACHE_DISABLED, "false"));
    
    String runRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT, runInfo.getRun());
    String cacheRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT, DataprocUtils.CDAP_CACHED_ARTIFACTS);
    
    String cdapVersion;
    if (cdapVersionInfo.isSnapshot()) {
      cdapVersion = String.format("%s.%s.%s-SNAPSHOT", cdapVersionInfo.getMajor(),
          cdapVersionInfo.getMinor(), cdapVersionInfo.getFix());
    } else {
      cdapVersion = String.format("%s.%s.%s", cdapVersionInfo.getMajor(),
          cdapVersionInfo.getMinor(), cdapVersionInfo.getFix());
    }

    LaunchMode launchMode = LaunchMode.valueOf(
        provisionerProperties.getOrDefault("launchMode", LaunchMode.CLIENT.name()).toUpperCase());

    DataprocMetric.Builder submitJobMetric =
        DataprocMetric.builder("provisioner.submitJob.response.count")
            .setRegion(region)
            .setLaunchMode(launchMode);

    try {
      if (disableLocalCaching) {
        tempDir = Files.createTempDirectory("dataproc.launcher").toFile();
      }
      List<LocalFile> localFiles = getRuntimeLocalFiles(runtimeJobInfo.getLocalizeFiles(), tempDir);
      LOG.warn("TEST_LOG: Prepared local files for upload. Total files count: {}", localFiles.size());

      List<Future<LocalFile>> uploadFutures = new ArrayList<>();
      for (LocalFile fileToUpload : localFiles) {
        boolean cacheable = gcsCacheEnabled && fileToUpload instanceof CacheableLocalFile;
        String targetFilePath = getPath(cacheable ? cacheRootPath : runRootPath, fileToUpload.getName());
        String targetFilePathWithVersion = getPath(cacheRootPath, cdapVersion, fileToUpload.getName());

        LOG.warn("TEST_LOG: Scheduling upload for file: {}, cacheable: {}, target path: {}",
                 fileToUpload.getName(), cacheable, targetFilePath);

        if (gcsCacheEnabled && artifactsCacheablePerCDAPVersion.contains(fileToUpload.getName())) {
          uploadFutures.add(
              provisionerContext.execute(
                      () -> uploadCacheableFile(bucket, targetFilePathWithVersion, fileToUpload))
                  .toCompletableFuture());
        } else {
          if (cacheable) {
            uploadFutures.add(
                provisionerContext.execute(
                        () -> uploadCacheableFile(bucket, targetFilePath, fileToUpload))
                    .toCompletableFuture());
          } else {
            uploadFutures.add(provisionerContext.execute(
                    () -> uploadFile(bucket, targetFilePath, fileToUpload, false))
                .toCompletableFuture());
          }
        }
      }

      List<LocalFile> uploadedFiles = new ArrayList<>();
      for (Future<LocalFile> uploadFuture : uploadFutures) {
        uploadedFiles.add(uploadFuture.get());
      }
      LOG.warn("TEST_LOG: Completed upload of all files. Total uploaded count: {}", uploadedFiles.size());

      CreateBatchRequest request = getCreateBatchRequest(runtimeJobInfo, uploadedFiles, launchMode);
      LOG.warn("TEST_LOG: Created Batch request. batchId: {}, parent: {}, mainClass: {}",
               request.getBatchId(), request.getParent(), request.getBatch().getSparkBatch().getMainClass());

      try {
        LOG.warn("TEST_LOG: Submitting Spark Batch creation to Dataproc Serverless API.");
        getBatchControllerClient().createBatchAsync(request);
        LOG.warn("TEST_LOG: Submitted successfully. Spark Batch runId: {}", runInfo.getRun());
      } catch (Exception ex) {
        LOG.warn("TEST_LOG: Exception while submitting Batch creation to Dataproc: ", ex);
        throw ex;
      }
      DataprocUtils.emitMetric(provisionerContext, submitJobMetric.build());
    } catch (Exception e) {
      LOG.warn("TEST_LOG: Exception in launch method: ", e);
      String errorReason = String.format("Error while launching serverless job %s.", getBatchId(runInfo));
      DataprocUtils.deleteGcsPath(getStorageClient(), bucket, runRootPath);
      DataprocUtils.emitMetric(provisionerContext, submitJobMetric.setException(e).build());
      
      ErrorCategory errorCategory = new ErrorCategory(ErrorCategoryEnum.STARTING);
      if (e instanceof ApiException) {
        int statusCode = ((ApiException) e).getStatusCode().getCode().getHttpStatusCode();
        ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
        throw new DataprocRuntimeException.Builder()
            .withCause(e)
            .withErrorCategory(errorCategory)
            .withErrorMessage(e.getMessage())
            .withErrorReason(DataprocUtils.getErrorReason(errorReason, e))
            .withErrorType(pair.getErrorType())
            .withErrorCodeType(ErrorCodeType.HTTP)
            .withErrorCode(String.valueOf(statusCode))
            .withDependency(true)
            .build();
      }
      throw new DataprocRuntimeException.Builder()
          .withErrorMessage(e.getMessage())
          .withErrorReason(errorReason)
          .withErrorCategory(errorCategory)
          .withCause(e)
          .build();
    } finally {
      if (disableLocalCaching) {
        DataprocUtils.deleteDirectoryContents(tempDir);
      }
    }
  }

  @Override
  public Optional<RuntimeJobDetail> getDetail(ProgramRunInfo programRunInfo) throws Exception {
    String batchId = getBatchId(programRunInfo);
    LOG.warn("TEST_LOG: Entering DataprocServerlessRuntimeJobManager.getDetail for runId: {}, batchId: {}",
             programRunInfo.getRun(), batchId);
    try {
      Batch batch = getBatchControllerClient().getBatch(GetBatchRequest.newBuilder()
          .setName(String.format("projects/%s/locations/%s/batches/%s", projectId, region, batchId))
          .build());
      RuntimeJobStatus jobStatus = getJobStatus(batch);
      String statusDetails = getJobStatusDetails(batch);
      LOG.warn("TEST_LOG: getDetail status response - batchId: {}, status: {}, details: {}",
               batchId, jobStatus, statusDetails);
      return Optional.of(new DataprocRuntimeJobDetail(
          getProgramRunInfo(batch),
          jobStatus,
          statusDetails,
          batchId));
    } catch (ApiException e) {
      LOG.warn("TEST_LOG: ApiException in getDetail for batchId: {}, statusCode: {}",
               batchId, e.getStatusCode().getCode());
      if (e.getStatusCode().getCode() != StatusCode.Code.NOT_FOUND) {
        int code = e.getStatusCode().getCode().getHttpStatusCode();
        ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(code);
        String errorReason = String.format("%s Unable to get details for serverless batch %s. %s",
            code, batchId, pair.getCorrectiveAction());
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.OTHERS),
            errorReason, e.getMessage(), pair.getErrorType(), true, ErrorCodeType.HTTP,
            String.valueOf(code), null, e);
      }
      LOG.debug("Dataproc serverless batch {} does not exist.", batchId);
    }
    return Optional.empty();
  }

  @Override
  public void stop(ProgramRunInfo programRunInfo) throws Exception {
    String batchId = getBatchId(programRunInfo);
    LOG.warn("TEST_LOG: Entering DataprocServerlessRuntimeJobManager.stop for runId: {}, batchId: {}",
             programRunInfo.getRun(), batchId);
    try {
      getBatchControllerClient().deleteBatch(DeleteBatchRequest.newBuilder()
          .setName(String.format("projects/%s/locations/%s/batches/%s", projectId, region, batchId))
          .build());
      LOG.warn("TEST_LOG: Successfully requested deletion of serverless batch: {}", batchId);
    } catch (ApiException e) {
      LOG.warn("TEST_LOG: ApiException in stop for batchId: {}, statusCode: {}", batchId, e.getStatusCode().getCode());
      if (e.getStatusCode().getCode() != StatusCode.Code.FAILED_PRECONDITION) {
        throw new Exception(String.format("Error occurred while stopping serverless batch %s.", batchId), e);
      }
      LOG.debug("Serverless batch {} is already deleted or stopping.", batchId);
    }
  }

  @Override
  public void kill(RuntimeJobDetail jobDetail) throws Exception {
    if (jobDetail != null) {
      stop(jobDetail.getRunInfo());
    }
  }

  @Override
  public void close() {
    BatchControllerClient client = this.batchControllerClient;
    if (client != null) {
      client.close();
    }
  }

  private List<LocalFile> getRuntimeLocalFiles(Collection<? extends LocalFile> runtimeLocalFiles,
                                               File tempDir) throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(tempDir);
    List<LocalFile> localFiles = new ArrayList<>(runtimeLocalFiles);
    localFiles.add(getTwillJar(locationFactory));
    localFiles.add(getLauncherJar(locationFactory));
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

  private boolean validateDeleteLifecycle(String bucketName, String run) {
    Storage storage = getStorageClient();
    Bucket bucket = storage.get(bucketName);
    for (BucketInfo.LifecycleRule rule : bucket.getLifecycleRules()) {
      if (rule.getAction() == null || rule.getCondition() == null
          || rule.getCondition().getDaysSinceCustomTime() == null) {
        continue;
      }
      if (rule.getAction() instanceof BucketInfo.LifecycleRule.DeleteLifecycleAction
          && rule.getCondition().getDaysSinceCustomTime() > 0) {
        if (!provisionerContext.getProperties()
            .containsKey(DataprocUtils.ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS)) {
          return true;
        }
        try {
          int timeBucketDays = Integer.parseInt(
              provisionerContext.getProperties()
                  .get(DataprocUtils.ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS));
          return rule.getCondition().getDaysSinceCustomTime() > timeBucketDays;
        } catch (NumberFormatException e) {
          return false;
        }
      }
    }
    return false;
  }

  private LocalFile uploadCacheableFile(String bucket, String targetFilePath, LocalFile localFile) throws IOException {
    Storage storage = getStorageClient();
    BlobId blobId = BlobId.of(bucket, targetFilePath);
    Blob blob = storage.get(blobId);
    LocalFile result;

    if (blob != null && blob.exists()) {
      if (artifactsCacheablePerCDAPVersion.contains(localFile.getName())
          && (blob.getUpdateTime() < cdapVersionInfo.getBuildTime())) {
        BlobInfo newBlobInfo = blob.toBuilder().setCustomTime(getCustomTime()).build();
        try {
          uploadToGcsUtil(localFile, storage, targetFilePath, newBlobInfo,
              Storage.BlobWriteOption.generationMatch(),
              Storage.BlobWriteOption.metagenerationMatch());
        } catch (StorageException e) {
          if (e.getCode() != HttpURLConnection.HTTP_PRECON_FAILED) {
            ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(e.getCode());
            String errorReason = String.format("%s Unable to upload file %s to GCS bucket gs://%s. %s",
                e.getCode(), localFile.getURI(), bucket, pair.getCorrectiveAction());
            throw ErrorUtils.getProgramFailureException(
                new ErrorCategory(ErrorCategoryEnum.STARTING), errorReason, e.getMessage(),
                pair.getErrorType(), true, ErrorCodeType.HTTP, String.valueOf(e.getCode()),
                GCS_DOC_URL, e);
          }
        }
      }
      result = new DefaultLocalFile(localFile.getName(),
          URI.create(String.format("gs://%s/%s", bucket, targetFilePath)),
          localFile.getLastModified(), localFile.getSize(),
          localFile.isArchive(), localFile.getPattern());
    } else {
      result = uploadFile(bucket, targetFilePath, localFile, true);
    }
    return result;
  }

  /**
   * Uploads a file to a GCS bucket, optionally caching it.
   */
  public LocalFile uploadFile(String bucket, String targetFilePath, LocalFile localFile,
                              boolean cacheable) throws IOException {
    BlobId blobId = BlobId.of(bucket, targetFilePath);
    String contentType = "application/octet-stream";
    BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
    if (cacheable) {
      long customTime = System.currentTimeMillis();
      if (artifactsCacheablePerCDAPVersion.contains(localFile.getName())) {
        customTime = getCustomTime();
      }
      blobInfoBuilder.setCustomTime(customTime);
    }
    BlobInfo blobInfo = blobInfoBuilder.setContentType(contentType).build();
    Storage storage = getStorageClient();
    Bucket bucketObj = storage.get(bucket);

    if (bucketObj == null) {
      String error = String.format("GCS Bucket '%s' does not exist", bucket);
      throw new ProgramFailureException.Builder()
          .withErrorCategory(new ErrorCategory(ErrorCategoryEnum.STARTING))
          .withErrorReason(error)
          .withErrorMessage(error)
          .withErrorType(ErrorType.USER)
          .build();
    }

    try {
      uploadToGcsUtil(localFile, storage, targetFilePath, blobInfo, Storage.BlobWriteOption.doesNotExist());
    } catch (StorageException e) {
      if (e.getCode() != HttpURLConnection.HTTP_PRECON_FAILED) {
        ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(e.getCode());
        String errorReason = String.format("%s Unable to upload file %s to GCS bucket gs://%s. %s",
            e.getCode(), localFile.getURI(), bucket, pair.getCorrectiveAction());
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.STARTING),
            errorReason, e.getMessage(), pair.getErrorType(), true,
            ErrorCodeType.HTTP, String.valueOf(e.getCode()), GCS_DOC_URL, e);
      }
      if (!cacheable) {
        Blob existingBlob = storage.get(blobId);
        BlobInfo newBlobInfo = BlobInfo.newBuilder(existingBlob.getBlobId()).setContentType(contentType).build();
        uploadToGcsUtil(localFile, storage, targetFilePath, newBlobInfo);
      }
    }

    return new DefaultLocalFile(localFile.getName(),
        URI.create(String.format("gs://%s/%s", bucket, targetFilePath)),
        localFile.getLastModified(), localFile.getSize(),
        localFile.isArchive(), localFile.getPattern());
  }

  private long getCustomTime() {
    return cdapVersionInfo.getBuildTime()
        + TimeUnit.DAYS.toMillis(cdapVersionInfo.isSnapshot() ? SNAPSHOT_EXPIRE_DAYS : EXPIRE_DAYS);
  }

  /**
   * Helper utility method to copy local files directly into GCS.
   */
  public void uploadToGcsUtil(LocalFile localFile, Storage storage, String targetFilePath, BlobInfo blobInfo,
      Storage.BlobWriteOption... blobWriteOptions) throws IOException {
    try (InputStream inputStream = openStream(localFile.getURI());
        WriteChannel writer = storage.writer(blobInfo, blobWriteOptions)) {
      ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
    }
  }

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
    return uri.toURL().openStream();
  }

  private CreateBatchRequest getCreateBatchRequest(RuntimeJobInfo runtimeJobInfo,
                                                   List<LocalFile> localFiles,
                                                   LaunchMode launchMode) throws IOException {
    List<String> jarUris = new ArrayList<>();
    List<String> fileUris = new ArrayList<>();
    for (LocalFile localFile : localFiles) {
      if (localFile.getName().endsWith("jar")) {
        jarUris.add(localFile.getURI().toString());
      } else {
        fileUris.add(localFile.getURI().toString());
      }
    }

    Map<String, String> sparkProperties = new LinkedHashMap<>();
    sparkProperties.putAll(getProperties(runtimeJobInfo));
    // Prepend CDAP launcher jar to the classpath settings for Spark
    sparkProperties.put("spark.driver.extraClassPath", "./" + Constants.Files.LAUNCHER_JAR);
    sparkProperties.put("spark.executor.extraClassPath", "./" + Constants.Files.LAUNCHER_JAR);

    String applicationJarLocalizedName = runtimeJobInfo.getArguments().get(Constants.Files.APPLICATION_JAR);
    SparkBatch sparkBatch = SparkBatch.newBuilder()
        .setMainClass(DataprocJobMain.class.getName())
        .addAllArgs(DataprocRuntimeJobManager.getArguments(
            runtimeJobInfo, localFiles, provisionerContext.getSparkCompat().getCompat(),
            applicationJarLocalizedName, launchMode))
        .addAllJarFileUris(jarUris)
        .addAllFileUris(fileUris)
        .build();

    RuntimeConfig runtimeConfig = RuntimeConfig.newBuilder()
        .putAllProperties(sparkProperties)
        .build();

    Batch.Builder batchBuilder = Batch.newBuilder()
        .setSparkBatch(sparkBatch)
        .setRuntimeConfig(runtimeConfig)
        .putAllLabels(labels);

    ExecutionConfig.Builder execConfigBuilder = ExecutionConfig.newBuilder();
    String serviceAccount = provisionerProperties.get("serviceAccount");
    String subnet = provisionerProperties.get("subnet");

    if (!Strings.isNullOrEmpty(serviceAccount)) {
      execConfigBuilder.setServiceAccount(serviceAccount);
    }
    if (!Strings.isNullOrEmpty(subnet)) {
      execConfigBuilder.setSubnetworkUri(subnet);
    }

    batchBuilder.setEnvironmentConfig(EnvironmentConfig.newBuilder()
        .setExecutionConfig(execConfigBuilder.build())
        .build());

    return CreateBatchRequest.newBuilder()
        .setParent(String.format("projects/%s/locations/%s", projectId, region))
        .setBatchId(getBatchId(runtimeJobInfo.getProgramRunInfo()))
        .setBatch(batchBuilder.build())
        .build();
  }

  private Map<String, String> getProperties(RuntimeJobInfo runtimeJobInfo) {
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    Map<String, String> properties = new LinkedHashMap<>();
    properties.put(CDAP_RUNTIME_NAMESPACE, runInfo.getNamespace());
    properties.put(CDAP_RUNTIME_APPLICATION, runInfo.getApplication());
    properties.put(CDAP_RUNTIME_VERSION, runInfo.getVersion());
    properties.put(CDAP_RUNTIME_PROGRAM, runInfo.getProgram());
    properties.put(CDAP_RUNTIME_PROGRAM_TYPE, runInfo.getProgramType());
    properties.put(CDAP_RUNTIME_RUNID, runInfo.getRun());
    return properties;
  }

  private ProgramRunInfo getProgramRunInfo(Batch batch) {
    Map<String, String> properties = batch.getRuntimeConfig().getPropertiesMap();
    return new ProgramRunInfo.Builder()
        .setNamespace(properties.get(CDAP_RUNTIME_NAMESPACE))
        .setApplication(properties.get(CDAP_RUNTIME_APPLICATION))
        .setVersion(properties.get(CDAP_RUNTIME_VERSION))
        .setProgramType(properties.get(CDAP_RUNTIME_PROGRAM_TYPE))
        .setProgram(properties.get(CDAP_RUNTIME_PROGRAM))
        .setRun(properties.get(CDAP_RUNTIME_RUNID))
        .build();
  }

  private RuntimeJobStatus getJobStatus(Batch batch) {
    Batch.State state = batch.getState();
    switch (state) {
      case STATE_UNSPECIFIED:
      case PENDING:
        return RuntimeJobStatus.STARTING;
      case RUNNING:
        return RuntimeJobStatus.RUNNING;
      case CANCELLING:
        return RuntimeJobStatus.STOPPING;
      case CANCELLED:
        return RuntimeJobStatus.STOPPED;
      case SUCCEEDED:
        return RuntimeJobStatus.COMPLETED;
      case FAILED:
        return RuntimeJobStatus.FAILED;
      default:
        throw new IllegalStateException(
            String.format("Unsupported state %s of serverless batch %s.", state, batch.getName()));
    }
  }

  @Nullable
  private String getJobStatusDetails(Batch batch) {
    return batch.getStateMessage();
  }

  private String getPath(String... pathSubComponents) {
    return Joiner.on("/").join(pathSubComponents);
  }

  public static String getBatchId(ProgramRunInfo runInfo) {
    // Generate a valid batch id (starts with letter, lowercase letters, numbers and hyphens only, max 63 chars)
    return "cdap-" + runInfo.getRun();
  }
}
