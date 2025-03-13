package io.cdap.cdap.runtime.spi.runtimejob;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.api.gax.rpc.ApiException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataproc.v1.Batch;
import com.google.cloud.dataproc.v1.BatchControllerClient;
import com.google.cloud.dataproc.v1.BatchControllerSettings;
import com.google.cloud.dataproc.v1.BatchOperationMetadata;
import com.google.cloud.dataproc.v1.EnvironmentConfig;
import com.google.cloud.dataproc.v1.ExecutionConfig;
import com.google.cloud.dataproc.v1.Job;
import com.google.cloud.dataproc.v1.JobControllerClient;
import com.google.cloud.dataproc.v1.LocationName;
import com.google.cloud.dataproc.v1.PeripheralsConfig;
import com.google.cloud.dataproc.v1.RuntimeConfig;
import com.google.cloud.dataproc.v1.SparkBatch;
import com.google.cloud.dataproc.v1.SparkHistoryServerConfig;
import com.google.cloud.dataproc.v1.SubmitJobRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.runtime.spi.CacheableLocalFile;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.VersionInfo;
import io.cdap.cdap.runtime.spi.common.DataprocMetric;
import io.cdap.cdap.runtime.spi.common.DataprocUtils;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.dataproc.DataprocRuntimeException;
import org.apache.twill.api.LocalFile;
import org.apache.twill.internal.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

public class ServerlessDataprocRuntimeJobManager extends DataprocRuntimeJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(ServerlessDataprocRuntimeJobManager.class);

  private static final Pattern DATAPROC_BATCH_ID_PATTERN = Pattern.compile("[a-z0-9][a-z0-9\\-]{2,61}[a-z0-9]");

  private final ProvisionerContext provisionerContext;
  private final String bucket;
  private final String region;
  private final Map<String, String> provisionerProperties;
  private final VersionInfo cdapVersionInfo;
  private final String projectId;
  private final Map<String, String> labels;
  //dataproc job labels (must match '[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}\p{N}_-]{0,62}' pattern)
  private static final String LABEL_CDAP_PROGRAM = "cdap-program";
  private static final String LABEL_CDAP_PROGRAM_TYPE = "cdap-program-type";
  private volatile BatchControllerClient batchControllerClient;
  private final GoogleCredentials credentials;
  private final String endpoint;


  /**
   * Created by dataproc provisioner with properties that are needed by dataproc runtime job
   * manager.
   *
   * @param clusterInfo           dataproc cluster information
   * @param provisionerProperties
   * @param cdapVersionInfo
   */
  public ServerlessDataprocRuntimeJobManager(DataprocClusterInfo clusterInfo,
                                             Map<String, String> provisionerProperties,
                                             VersionInfo cdapVersionInfo) {

    super(clusterInfo, provisionerProperties, cdapVersionInfo);
    this.provisionerContext = clusterInfo.getProvisionerContext();
    this.bucket = clusterInfo.getBucket();
    this.region = clusterInfo.getRegion();
    this.cdapVersionInfo = cdapVersionInfo;
    this.provisionerProperties = provisionerProperties;
    this.projectId = clusterInfo.getProjectId();
    this.labels = clusterInfo.getLabels();
    this.credentials = clusterInfo.getCredentials();
    this.endpoint = clusterInfo.getEndpoint();
  }

  @Override
  public void launch(RuntimeJobInfo runtimeJobInfo) throws Exception {
    String bucket = DataprocUtils.getBucketName(this.bucket);
    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();

    // Caching is disabled if it's been explicitly disabled or delete lifecycle is not set on the bucket.
    boolean gcsCacheEnabled = Boolean.parseBoolean(
      provisionerContext.getProperties().getOrDefault(DataprocUtils.GCS_CACHE_ENABLED, "true"))
      || !validateDeleteLifecycle(bucket, runInfo.getRun());

    LOG.debug(
      "Launching run {} with following configurations:  project {}, region {}, bucket {}.",
      runInfo.getRun(), projectId, region, bucket);
    if (!gcsCacheEnabled) {
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
    String cacheRootPath = getPath(DataprocUtils.CDAP_GCS_ROOT,
                                   DataprocUtils.CDAP_CACHED_ARTIFACTS);
    String cdapVersion;
    if (cdapVersionInfo.isSnapshot()) {
      cdapVersion = String.format("%s.%s.%s-SNAPSHOT", cdapVersionInfo.getMajor(),
                                  cdapVersionInfo.getMinor(),
                                  cdapVersionInfo.getFix());
    } else {
      cdapVersion = String.format("%s.%s.%s", cdapVersionInfo.getMajor(),
                                  cdapVersionInfo.getMinor(),
                                  cdapVersionInfo.getFix());
    }

    LaunchMode launchMode = LaunchMode.valueOf(
      provisionerProperties.getOrDefault("launchMode", LaunchMode.CLUSTER.name()).toUpperCase());
    DataprocMetric.Builder submitJobMetric =
      DataprocMetric.builder("provisioner.submitJob.response.count")
        .setRegion(region)
        .setLaunchMode(launchMode);
    try {
      // step 1: build twill.jar and launcher.jar and add them to files to be copied to gcs
      if (disableLocalCaching) {
        LOG.debug("Local caching is disabled, "
                    + "continuing without caching twill and dataproc launcher jars.");
        tempDir = Files.createTempDirectory("dataproc.launcher").toFile();
      }
      List<LocalFile> localFiles = getRuntimeLocalFiles(runtimeJobInfo.getLocalizeFiles(), tempDir);

      // step 2: upload all the necessary files to gcs so that those files are available to dataproc job
      List<Future<LocalFile>> uploadFutures = new ArrayList<>();
      for (LocalFile fileToUpload : localFiles) {
        boolean cacheable = gcsCacheEnabled && fileToUpload instanceof CacheableLocalFile;
        String targetFilePath = getPath(cacheable ? cacheRootPath : runRootPath,
                                        fileToUpload.getName());
        String targetFilePathWithVersion = getPath(cacheRootPath, cdapVersion,
                                                   fileToUpload.getName());

        if (gcsCacheEnabled && artifactsCacheablePerCDAPVersion.contains(fileToUpload.getName())) {
          // upload artifacts cacheable per cdap version to <bucket>/cdap-job/cached-artifacts/<cdapVersion>/
          uploadFutures.add(
            provisionerContext.execute(
                () -> uploadCacheableFile(bucket, targetFilePathWithVersion, fileToUpload))
              .toCompletableFuture());
        } else {
          if (cacheable) {
            // upload cacheable artifacts to <bucket>/cdap-job/cached-artifacts/
            uploadFutures.add(
              provisionerContext.execute(
                  () -> uploadCacheableFile(bucket, targetFilePath, fileToUpload))
                .toCompletableFuture());
          } else {
            // non-cacheable artifacts to <bucket>/cdap-job/<runid>/
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

      // step 3: build the hadoop job request to be submitted to dataproc
      Batch batch = getSubmitBatchRequest(runtimeJobInfo, uploadedFiles);
      // step 4: submit hadoop job to dataproc
      try {
        LocationName locationName = LocationName.newBuilder()
          .setProject(projectId).setLocation(region).build();
        OperationFuture<Batch, BatchOperationMetadata> submitJobAsOperationAsyncRequest =
          getBatchControllerClient().createBatchAsync(locationName, batch, getJobId(runInfo));
        LOG.warn("SANKET : afterjobsumbit");
        LOG.warn("Successfully submitted BATCH job {} to Serverless",
                 submitJobAsOperationAsyncRequest.get().getName());
      } catch (AlreadyExistsException ex) {
        //the job id already exists, ignore the job.
        LOG.warn("The dataproc job {} already exists. Ignoring resubmission of the job.",
                 getJobId(runInfo));
      }
      DataprocUtils.emitMetric(provisionerContext, submitJobMetric.build());
    } catch (Exception e) {
      String errorReason = String.format("Error while launching job %s on Serverless Dataproc.", getJobId(runInfo));
      // delete all uploaded gcs files in case of exception
      DataprocUtils.deleteGcsPath(getStorageClient(), bucket, runRootPath);
      DataprocUtils.emitMetric(provisionerContext, submitJobMetric.setException(e).build());
      // ResourceExhaustedException indicates Dataproc agent running on master node
      // isn't emitting heartbeat. This usually indicates master VM crashing due to OOM.
      ErrorCategory errorCategory = new ErrorCategory(ErrorCategory.ErrorCategoryEnum.STARTING);
      if (e instanceof ApiException) {
        int statusCode =
          ((ApiException) e).getStatusCode().getCode().getHttpStatusCode();
        ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
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
    String jobId = getJobId(programRunInfo);
    try {
      LOG.warn(" SANKET : in  : jobId : {} : projectId : {} , region : {}", jobId, projectId, region);

      //TODO ::  Just after "batchControllerClient.createBatchAsync" the below line may give NOT_FOUND . Need to figure
      // how to handle this

      Batch batch = getBatchControllerClient().getBatch(getFullBatchName(projectId, region, jobId));
      return Optional.of(new DataprocRuntimeJobDetail(getProgramRunInfo(batch),
                                                      getRuntimeJobStatus(batch),
                                                      getJobStatusDetails(batch)));
    } catch (ApiException e) {
      /*
      LOG.warn(" SANKET : e.getStatusCode().getCode() : " + e.getStatusCode().getCode());
      if (e.getStatusCode().getCode() != StatusCode.Code.NOT_FOUND
      || e.getStatusCode().getCode() != StatusCode.Code.CANCELLED) {
        throw new Exception(String.format("Error while getting details for job %s on cluster %s.",
            jobId, clusterName), e);
      }
      // Status is not found if job is finished or manually deleted by the user
      LOG.debug("Dataproc job {} does not exist in project {}, region {}.", jobId, projectId,
          region);*/
    }
    return Optional.empty();
  }

  /**
   * Returns job state details, such as an error description if the state is ERROR. For other job
   * states, returns null.
   */
  @Nullable
  private String getJobStatusDetails(Batch job) {
    return job.getState().name(); //TODO : Check for better details
  }


  private ProgramRunInfo getProgramRunInfo(Batch batch) {
    Map<String, String> jobProperties = batch.getRuntimeConfig().getPropertiesMap();

    ProgramRunInfo.Builder builder = new ProgramRunInfo.Builder()
      .setNamespace(jobProperties.get(CDAP_RUNTIME_NAMESPACE))
      .setApplication(jobProperties.get(CDAP_RUNTIME_APPLICATION))
      .setVersion(jobProperties.get(CDAP_RUNTIME_VERSION))
      .setProgramType(jobProperties.get(CDAP_RUNTIME_PROGRAM_TYPE))
      .setProgram(jobProperties.get(CDAP_RUNTIME_PROGRAM))
      .setRun(jobProperties.get(CDAP_RUNTIME_RUNID));
    return builder.build();
  }

  private String getFullBatchName(String project, String region, String jobId){
    return String.format("projects/%s/locations/%s/batches/%s", project, region, jobId);
  }


  /**
   * Returns {@link RuntimeJobStatus}.
   */
  private RuntimeJobStatus getRuntimeJobStatus(Batch batch) {
    Batch.State state = batch.getState();
    RuntimeJobStatus runtimeJobStatus;
    switch (state) {
      case STATE_UNSPECIFIED:
      case PENDING:
        runtimeJobStatus = RuntimeJobStatus.STARTING;
        break;
      case RUNNING:
        runtimeJobStatus = RuntimeJobStatus.RUNNING;
        break;
      case SUCCEEDED:
        runtimeJobStatus = RuntimeJobStatus.COMPLETED;
        break;
      case CANCELLING:
        runtimeJobStatus = RuntimeJobStatus.STOPPING;
        break;
      case CANCELLED:
        runtimeJobStatus = RuntimeJobStatus.STOPPED;
        break;
      case FAILED:
        runtimeJobStatus = RuntimeJobStatus.FAILED;
        break;
      default:
        // this needed for ATTEMPT_FAILURE state which is a state for restartable job. Currently we do not launch
        // restartable jobs
        throw new IllegalStateException(
          String.format("Unsupported job state %s of the dataproc job %s ", batch.getState(),
                        batch.getName()));
    }
    return runtimeJobStatus;
  }



  /**
   * Creates and returns dataproc job submit request.
   */
  private Batch getSubmitBatchRequest(RuntimeJobInfo runtimeJobInfo,
                                      List<LocalFile> localFiles) {
    String applicationJarLocalizedName = runtimeJobInfo.getArguments().get(Constants.Files.APPLICATION_JAR);

    LaunchMode launchMode = LaunchMode.valueOf(
      provisionerProperties.getOrDefault("launchMode", LaunchMode.CLIENT.name()).toUpperCase());

    SparkBatch.Builder sparkBatchBuilder =
      SparkBatch.newBuilder()
        .setMainClass(DataprocJobMain.class.getName())
        .addAllArgs(getArguments(runtimeJobInfo, localFiles, provisionerContext.getSparkCompat().getCompat(),
                                 applicationJarLocalizedName, launchMode));

    for (LocalFile localFile : localFiles) {
      // add jar file
      URI uri = localFile.getURI();
      if (localFile.getName().endsWith("jar")) {
        sparkBatchBuilder.addJarFileUris(uri.toString());
      } else {
        sparkBatchBuilder.addFileUris(uri.toString());
      }
    }
//
//    // MANUAL ADDING JARS FOR TEST
//    String[] fileUris = {
//      "gs://serverlessdataproc/sanket_lib/ch.qos.logback.logback-classic-1.2.11.jar",
//      "gs://serverlessdataproc/sanket_lib/ch.qos.logback.logback-core-1.2.11.jar",
//      "gs://serverlessdataproc/sanket_lib/com.101tec.zkclient-0.10.jar",
//      "gs://serverlessdataproc/sanket_lib/com.google.code.findbugs.jsr305-2.0.1.jar",
//      "gs://serverlessdataproc/sanket_lib/com.google.code.gson.gson-2.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/com.google.errorprone.error_prone_annotations-2.18.0.jar",
//      "gs://serverlessdataproc/sanket_lib/com.google.guava.guava-20.0.jar",
//      "gs://serverlessdataproc/sanket_lib/com.yammer.metrics.metrics-core-2.2.0.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-api-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-common-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-core-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-discovery-api-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-discovery-core-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-yarn-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.cdap.twill.twill-zookeeper-1.3.1.jar",
//      "gs://serverlessdataproc/sanket_lib/io.netty.netty-buffer-4.1.75.Final.jar",
//      "gs://serverlessdataproc/sanket_lib/io.netty.netty-codec-4.1.75.Final.jar",
//      "gs://serverlessdataproc/sanket_lib/io.netty.netty-codec-http-4.1.75.Final.jar",
//      "gs://serverlessdataproc/sanket_lib/io.netty.netty-common-4.1.75.Final.jar",
//      "gs://serverlessdataproc/sanket_lib/io.netty.netty-transport-4.1.75.Final.jar",
//      "gs://serverlessdataproc/sanket_lib/lib-ch.qos.logback.logback-classic-1.2.11.jar",
//      "gs://serverlessdataproc/sanket_lib/net.sf.jopt-simple.jopt-simple-3.2.jar",
//      "gs://serverlessdataproc/sanket_lib/org.apache.kafka.kafka-clients-0.10.2.2.jar",
//      "gs://serverlessdataproc/sanket_lib/org.apache.kafka.kafka_2.12-0.10.2.2.jar",
//      "gs://serverlessdataproc/sanket_lib/org.scala-lang.modules.scala-parser-combinators_2.12-1.0.4.jar",
//      "gs://serverlessdataproc/sanket_lib/org.scala-lang.scala-library-2.12.15.jar",
//      "gs://serverlessdataproc/sanket_lib/org.slf4j.slf4j-api-1.7.15.jar"
//    };
//
//    for(String uri : fileUris) {
//      LOG.info(" SANKET ADDING FILE : {}", uri);
//      sparkBatchBuilder.addJarFileUris(uri);
//    }

    // TODO : HARDCODED PROPS : Need to define flow for this


    ExecutionConfig executionConfig = ExecutionConfig.newBuilder()
      .setNetworkUri("default")
      .setSubnetworkUri("pga-subnet")
      .build();

//    //TODO : To make this an advanced option via UI
//    SparkHistoryServerConfig sparkHistoryServerConfig = SparkHistoryServerConfig.newBuilder()
//      .setDataprocCluster("projects/cdf-test-317207/regions/us-west1/clusters/sanket-spark-history").build();

//    PeripheralsConfig peripheralsConfig = PeripheralsConfig.newBuilder()
//      .setSparkHistoryServerConfig(sparkHistoryServerConfig)
//      .build();


    EnvironmentConfig environmentConfig = EnvironmentConfig.newBuilder()
      .setExecutionConfig(executionConfig)
//      .setPeripheralsConfig(peripheralsConfig)
      .build();

    RuntimeConfig runtimeConfig = RuntimeConfig.newBuilder()
      .setVersion("1.1")
      .putAllProperties(getProperties(runtimeJobInfo)).build();

    ProgramRunInfo runInfo = runtimeJobInfo.getProgramRunInfo();
    Batch.Builder dataprocBatchBuilder = Batch.newBuilder()
      // use program run uuid as hadoop job id on dataproc
      // place the job on provisioned cluster
//        .setPlacement(JobPlacement.newBuilder().setClusterName(clusterName).build()) //TODO figure out the use
      // add same labels as provisioned cluster
      .putAllLabels(labels)
      // Job label values must match the pattern '[\p{Ll}\p{Lo}\p{N}_-]{0,63}'
      // Since program name and type are class names they should follow that pattern once we remove all
      // capitals
      .putLabels(LABEL_CDAP_PROGRAM, runInfo.getProgram().toLowerCase())
      .putLabels(LABEL_CDAP_PROGRAM_TYPE, runInfo.getProgramType().toLowerCase())
      .setRuntimeConfig(runtimeConfig)
      .setEnvironmentConfig(environmentConfig)
      .setSparkBatch(sparkBatchBuilder.build());

    return dataprocBatchBuilder.build();

  }


  /**
   * Returns a {@link JobControllerClient} to interact with Dataproc Job API.
   */
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

      // instantiate a dataproc job controller client
      CredentialsProvider credentialsProvider = FixedCredentialsProvider.create(credentials);
      this.batchControllerClient = client = BatchControllerClient.create(
        BatchControllerSettings.newBuilder().setCredentialsProvider(credentialsProvider)
          .setEndpoint(String.format("%s-%s", region, endpoint)).build());
    }
    return client;
  }

  public static String getJobId(ProgramRunInfo runInfo) {
    List<String> parts = ImmutableList.of(
      runInfo.getNamespace().substring(0,Math.min(runInfo.getNamespace().length(),5)).toLowerCase(),
      runInfo.getApplication().substring(0,Math.min(runInfo.getApplication().length(),15)).toLowerCase(),
      runInfo.getProgram().toLowerCase());
    String joined = Joiner.on("-").join(parts);
    joined = joined.substring(0, Math.min(joined.length(), 26));
    joined = joined + "-" + runInfo.getRun();
    if (!DATAPROC_BATCH_ID_PATTERN.matcher(joined).matches()) {
      throw new IllegalArgumentException(
        String.format("Job ID %s is not a valid dataproc job id. ", joined));
    }

    //A batch ID must start and end in a letter or a number, be between 4 and 63 characters long, and contain only
    //lowercase letters, numbers, and hyphens


    return joined;
  }


}
