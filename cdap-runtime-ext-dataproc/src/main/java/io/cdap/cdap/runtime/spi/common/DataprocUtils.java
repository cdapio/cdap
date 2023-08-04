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

package io.cdap.cdap.runtime.spi.common;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerMetrics;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains common methods that are needed by DataprocProvisioner and
 * DataprocRuntimeJobManager.
 */
public final class DataprocUtils {

  // The property name for the GCS bucket used by the runtime job manager for launching jobs via the job API
  // It can be overridden by profile runtime arguments (system.profile.properties.bucket)
  public static final String BUCKET = "bucket";
  public static final String CDAP_GCS_ROOT = "cdap-job";
  public static final String CDAP_CACHED_ARTIFACTS = "cached-artifacts";
  public static final String WORKER_CPU_PREFIX = "Up to";
  // The property name for disabling caching of artifacts in GCS uploaded to GCS Bucket used by Dataproc.
  // It can be overridden by profile runtime arguments (system.profile.properties.gcsCacheEnabled)
  public static final String GCS_CACHE_ENABLED = "gcsCacheEnabled";
  public static final String ARTIFACTS_COMPUTE_HASH_TIME_BUCKET_DAYS = "app.artifact.compute.hash.time.bucket.days";
  public static final Path CACHE_DIR_PATH = Paths.get(System.getProperty("java.io.tmpdir"),
      "dataproc.launcher.cache");
  public static final String LOCAL_CACHE_DISABLED = "disableLocalCaching";
  private static final int NUMBER_OF_RETRIES = 5;
  private static final int MIN_WAIT_TIME_MILLISECOND = 500;
  private static final int MAX_WAIT_TIME_MILLISECOND = 10000;

  private static final SplittableRandom RANDOM = new SplittableRandom();
  private static final int SET_CUSTOM_TIME_MAX_RETRY = 6;
  private static final int SET_CUSTOM_TIME_MAX_SLEEP_MILLIS_BEFORE_RETRY = 20000;

  public static final String TROUBLESHOOTING_DOCS_URL_KEY = "troubleshootingDocsURL";
  // Empty url will ensure help messages don't appear by default in Dataproc error messages.
  // This property needs to be overridden in cdap-site.
  public static final String TROUBLESHOOTING_DOCS_URL_DEFAULT = "";

  /**
   * resources required by Runtime Job (io.cdap.cdap.runtime.spi.runtimejob.RuntimeJob) that will be
   * running on driver pool nodes.
   */
  public static final String DRIVER_MEMORY_MB = "driverMemoryMB";
  public static final String DRIVER_MEMORY_MB_DEFAULT = "2048";
  public static final String DRIVER_VCORES = "driverVCores";
  public static final String DRIVER_VCORES_DEFAULT = "1";

  public static final String GCS_HTTP_REQUEST_CONNECTION_TIMEOUT_MILLIS =
      "gcs.http.request.connection.timeout.mills";
  public static final String GCS_HTTP_REQUEST_CONNECTION_TIMEOUT_MILLIS_DEFAULT = "60000";
  public static final String GCS_HTTP_REQUEST_READ_TIMEOUT_MILLIS =
      "gcs.http.request.read.timeout.mills";
  public static final String GCS_HTTP_REQUEST_READ_TIMEOUT_MILLIS_DEFAULT = "60000";
  public static final String GCS_HTTP_REQUEST_TOTAL_TIMEOUT_MINS =
      "gcs.http.request.total.timeout.mins";
  public static final String GCS_HTTP_REQUEST_TOTAL_TIMEOUT_MINS_DEFAULT = "5";

  /**
   * HTTP Status-Code 429: RESOURCE_EXHAUSTED.
   */
  public static final int RESOURCE_EXHAUSTED = 403;

  private static final Logger LOG = LoggerFactory.getLogger(DataprocUtils.class);
  private static final String GS_PREFIX = "gs://";

  // keys and values cannot be longer than 63 characters
  // keys and values can only contain lowercase letters, numbers, underscores, and dashes
  // keys must start with a lowercase letter
  // keys cannot be empty
  private static final Pattern LABEL_KEY_PATTERN = Pattern.compile("^[a-z][a-z0-9_-]{0,62}$");
  private static final Pattern LABEL_VAL_PATTERN = Pattern.compile("^[a-z0-9_-]{0,63}$");

  /**
   * Deletes provided directory path on GCS.
   *
   * @param storageClient storage client
   * @param bucket bucket
   * @param path dir path to delete
   */
  public static void deleteGcsPath(Storage storageClient, String bucket, String path) {
    try {
      String bucketName = getBucketName(bucket);
      StorageBatch batch = storageClient.batch();
      Page<Blob> blobs = storageClient.list(bucketName, Storage.BlobListOption.currentDirectory(),
          Storage.BlobListOption.prefix(path + "/"));
      boolean addedToDelete = false;
      for (Blob blob : blobs.iterateAll()) {
        LOG.trace("Added path to be deleted {}", blob.getName());
        batch.delete(blob.getBlobId(), Storage.BlobSourceOption.generationMatch());
        addedToDelete = true;
      }

      if (addedToDelete) {
        batch.submit();
      }
    } catch (Exception e) {
      LOG.warn(String.format("GCS path %s was not cleaned up for bucket %s due to %s. ",
          path, bucket, e.getMessage()), e);
    }
  }

  /**
   * Removes prefix gs:// and returns bucket name.
   */
  public static String getBucketName(String bucket) {
    if (bucket.startsWith(GS_PREFIX)) {
      return bucket.substring(GS_PREFIX.length());
    }
    return bucket;
  }

  /**
   * Utility class to parse the keyvalue string from UI Widget and return back HashMap. String is of
   * format  {@code <key><keyValueDelimiter><value><delimiter><key><keyValueDelimiter><value>} eg:
   * networktag1=out2internet;networktag2=priority The return from the method is a map with key
   * value pairs of (networktag1 out2internet) and (networktag2 priority)
   *
   * @param configValue String to be parsed into key values format
   * @param delimiter Delimiter used for keyvalue pairs
   * @param keyValueDelimiter Delimiter between key and value.
   * @return Map of Key value pairs parsed from input configValue using the delimiters.
   */
  public static Map<String, String> parseKeyValueConfig(@Nullable String configValue,
      String delimiter,
      String keyValueDelimiter) throws IllegalArgumentException {
    Map<String, String> map = new HashMap<>();
    if (configValue == null) {
      return map;
    }
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter, 2);
      String key = parts[0].trim();
      String value = parts.length > 1 ? parts[1].trim() : "";
      map.put(key, value);
    }
    return map;
  }

  /**
   * Parses the given list of IP CIDR blocks into list of {@link IPRange}.
   */
  public static List<IPRange> parseIpRanges(List<String> ranges) {
    return ranges.stream().map(IPRange::new).collect(Collectors.toList());
  }

  /**
   * Get network from the metadata server.
   */
  public static String getSystemNetwork() {
    try {
      String network = getMetadata("instance/network-interfaces/0/network");
      // will be something like projects/<project-number>/networks/default
      return network.substring(network.lastIndexOf('/') + 1);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get the network from the environment. "
          + "Please explicitly set the network.", e);
    }
  }

  /**
   * Get zone from the metadata server.
   */
  public static String getSystemZone() {
    try {
      String zone = getMetadata("instance/zone");
      // will be something like projects/<project-number>/zones/us-east1-b
      return zone.substring(zone.lastIndexOf('/') + 1);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get the zone from the environment. "
          + "Please explicitly set the zone.", e);
    }
  }

  /**
   * Returns the region of the given zone.
   */
  public static String getRegionFromZone(String zone) {
    int idx = zone.lastIndexOf("-");
    if (idx <= 0) {
      throw new IllegalArgumentException(
          "Invalid zone. Zone must be in the format of <region>-<zone-name>");
    }
    return zone.substring(0, idx);
  }

  /**
   * Get project id from the metadata server.
   */
  public static String getSystemProjectId() {
    try {
      return getMetadata("project/project-id");
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to get project id from the environment. "
          + "Please explicitly set the project id and account key.", e);
    }
  }

  /**
   * Emit a dataproc metric.
   **/
  public static void emitMetric(ProvisionerContext context, String region,
      String metricName, @Nullable Exception e) {
    StatusCode.Code statusCode;
    if (e == null) {
      statusCode = StatusCode.Code.OK;
    } else {
      Throwable cause = e.getCause();
      if (cause instanceof ApiException) {
        ApiException apiException = (ApiException) cause;
        statusCode = apiException.getStatusCode().getCode();
      } else {
        statusCode = StatusCode.Code.INTERNAL;
      }
    }
    Map<String, String> tags = ImmutableMap.<String, String>builder()
        .put("reg", region)
        .put("sc", statusCode.toString())
        .build();
    ProvisionerMetrics metrics = context.getMetrics(tags);
    metrics.count(metricName, 1);
  }

  public static void emitMetric(ProvisionerContext context, String region, String metricName) {
    emitMetric(context, region, metricName, null);
  }

  /**
   * Makes a request to the metadata server that lives on the VM, as described at
   * https://cloud.google.com/compute/docs/storing-retrieving-metadata.
   */
  private static String getMetadata(String resource) throws IOException {
    URL url = new URL("http://metadata.google.internal/computeMetadata/v1/" + resource);
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) url.openConnection();
      connection.setRequestProperty("Metadata-Flavor", "Google");
      connection.connect();
      try (Reader reader = new InputStreamReader(connection.getInputStream(),
          StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  /**
   * Recursively deletes all the contents of the directory and the directory itself.
   */
  public static void deleteDirectoryContents(@Nullable File file) {
    if (file == null) {
      return;
    }

    if (file.isDirectory()) {
      File[] entries = file.listFiles();
      if (entries != null) {
        for (File entry : entries) {
          deleteDirectoryContents(entry);
        }
      }
    }
    if (!file.delete()) {
      LOG.warn("Failed to delete file {}.", file);
    }
  }

  /**
   * Recursively deletes all the contents of the directory and the directory itself with retries.
   */
  public static synchronized void deleteDirectoryWithRetries(@Nullable File file,
      String errorMessageOnFailure) {
    ExponentialBackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(MIN_WAIT_TIME_MILLISECOND)
        .setMaxIntervalMillis(MAX_WAIT_TIME_MILLISECOND).build();

    Exception exception = null;
    int counter = 0;
    while (counter < NUMBER_OF_RETRIES) {
      counter++;

      try {
        deleteDirectoryContents(file);
        return;
      } catch (Exception e) {
        // exception does not get logged since it might get too chatty.
        exception = e;
      }

      try {
        Thread.sleep(backOff.nextBackOffMillis());
      } catch (InterruptedException | IOException e) {
        exception = e;
        break;
      }
    }
    throw new RuntimeException(String.format(errorMessageOnFailure, file), exception);
  }

  public static void setTemporaryHoldOnGcsObject(Storage storage, String bucket, Blob blob,
      String targetFilePath) throws InterruptedException {
    updateTemporaryHoldOnGcsObject(storage, bucket, blob, blob.getBlobId(), targetFilePath, true);
  }

  public static void removeTemporaryHoldOnGcsObject(Storage storage, String bucket, BlobId blobId,
      String targetFilePath) throws InterruptedException {
    updateTemporaryHoldOnGcsObject(storage, bucket, null, blobId, targetFilePath, false);
  }

  private static void updateTemporaryHoldOnGcsObject(Storage storage, String bucket,
      @Nullable Blob blob, BlobId blobId,
      String targetFilePath,
      boolean temporaryHold) throws InterruptedException {
    for (int i = 1; i <= SET_CUSTOM_TIME_MAX_RETRY; i++) {
      try {
        // get a random jitter between 30min to 90min
        long jitter = TimeUnit.MINUTES.toMillis(RANDOM.nextInt(60)) + TimeUnit.MINUTES.toMillis(30);
        // Blob can be null when we set temporary hold to false as we don't need to check pre-existing custom time.
        // When setting to true, we'll check if custom time was recently set in which case we'll skip this operation.
        assert temporaryHold == (blob != null);
        if (!temporaryHold || blob.getCustomTime() == null
            || blob.getTemporaryHold() == null || !blob.getTemporaryHold().booleanValue()
            || blob.getCustomTime() + jitter < System.currentTimeMillis()) {
          BlobInfo blobInfo = BlobInfo.newBuilder(blobId)
              .setCustomTime(System.currentTimeMillis())
              .setTemporaryHold(temporaryHold)
              .build();
          storage.update(blobInfo);

          LOG.debug("Successfully set custom time for gs://{}/{} and temporary hold to {}", bucket,
              targetFilePath,
              temporaryHold);
        } else {
          //custom time is still fresh
          LOG.debug("Skip setting custom time for gs://{}/{} since it is fresh", bucket,
              targetFilePath);
        }
        return;
      } catch (Exception ex) {
        if (i == SET_CUSTOM_TIME_MAX_RETRY) {
          throw ex;
        }
        Thread.sleep(RANDOM.nextInt(SET_CUSTOM_TIME_MAX_SLEEP_MILLIS_BEFORE_RETRY));
      }
    }
  }

  public static String getTroubleshootingHelpMessage(@Nullable String troubleshootingDocsUrl) {
    if (Strings.isNullOrEmpty(troubleshootingDocsUrl)) {
      return "";
    }
    return String.format("For troubleshooting Dataproc errors, refer to %s",
        troubleshootingDocsUrl);
  }

  private DataprocUtils() {
    // no-op
  }
}
