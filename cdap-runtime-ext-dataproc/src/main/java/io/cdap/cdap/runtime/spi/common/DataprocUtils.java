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

import com.google.api.gax.paging.Page;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * This class contains common methods that are needed by DataprocProvisioner and DataprocRuntimeJobManager.
 */
public final class DataprocUtils {

  public static final String CDAP_GCS_ROOT = "cdap-job";

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
  public static void deleteGCSPath(Storage storageClient, String bucket, String path) {
    try {
      String bucketName = getBucketName(bucket);
      StorageBatch batch = storageClient.batch();
      Page<Blob> blobs = storageClient.list(bucketName, Storage.BlobListOption.currentDirectory(),
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
    } catch (Exception e) {
      LOG.warn(String.format("GCS path %s was not cleaned up for bucket %s due to %s. ",
                             path, bucket, e.getMessage()), e);
    }
  }

  /**
   * Removes prefix gs:// and returns bucket name
   */
  public static String getBucketName(String bucket) {
    if (bucket.startsWith(GS_PREFIX)) {
      return bucket.substring(GS_PREFIX.length());
    }
    return bucket;
  }

  /**
   * Utilty class to parse the keyvalue string from UI Widget and return back HashMap.
   * String is of format  <key><keyValueDelimiter><value><delimiter><key><keyValueDelimiter><value>
   * eg:  networktag1=out2internet;networktag2=priority
   * The return from the method is a map with key value pairs of (networktag1 out2internet) and (networktag2 priority)
   *
   * @param configValue String to be parsed into key values format
   * @param delimiter Delimiter used for keyvalue pairs
   * @param keyValueDelimiter Delimiter between key and value.
   * @return Map of Key value pairs parsed from input configValue using the delimiters.
   */
  public static Map<String, String> parseKeyValueConfig(@Nullable String configValue, String delimiter,
                                                        String keyValueDelimiter) throws IllegalArgumentException {
    Map<String, String> map = new HashMap<>();
    if (configValue == null) {
      return map;
    }
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter, 2);
      if (parts.length != 2) {
        throw new IllegalArgumentException("Invalid KeyValue " + property);
      }
      String key = parts[0];
      String value = parts[1];
      map.put(key, value);
    }
    return map;
  }

  /**
   * Parses labels that are expected to be of the form key1=val1,key2=val2 into a map of key values.
   *
   * If a label key or value is invalid, a message will be logged but the key-value will not be returned in the map.
   * Keys and values cannot be longer than 63 characters.
   * Keys and values can only contain lowercase letters, numeric characters, underscores, and dashes.
   * Keys must start with a lowercase letter and must not be empty.
   *
   * If a label is given without a '=', the label value will be empty.
   * If a label is given as 'key=', the label value will be empty.
   * If a label has multiple '=', it will be ignored. For example, 'key=val1=val2' will be ignored.
   *
   * @param labelsStr the labels string to parse
   * @return valid labels from the parsed string
   */
  public static Map<String, String> parseLabels(String labelsStr) {
    Splitter labelSplitter = Splitter.on(',').trimResults().omitEmptyStrings();
    Splitter kvSplitter = Splitter.on('=').trimResults().omitEmptyStrings();

    Map<String, String> validLabels = new HashMap<>();
    for (String keyvalue : labelSplitter.split(labelsStr)) {
      Iterator<String> iter = kvSplitter.split(keyvalue).iterator();
      if (!iter.hasNext()) {
        continue;
      }
      String key = iter.next();
      String val = iter.hasNext() ? iter.next() : "";
      if (iter.hasNext()) {
        LOG.warn("Ignoring invalid label {}. Labels should be of the form 'key=val' or just 'key'", keyvalue);
        continue;
      }
      if (!LABEL_KEY_PATTERN.matcher(key).matches()) {
        LOG.warn("Ignoring invalid label key {}. Label keys cannot be longer than 63 characters, must start with "
                   + "a lowercase letter, and can only contain lowercase letters, numeric characters, underscores,"
                   + " and dashes.", key);
        continue;
      }
      if (!LABEL_VAL_PATTERN.matcher(val).matches()) {
        LOG.warn("Ignoring invalid label value {}. Label values cannot be longer than 63 characters, "
                   + "and can only contain lowercase letters, numeric characters, underscores, and dashes.", val);
        continue;
      }
      validLabels.put(key, val);
    }
    return validLabels;
  }

  /**
   * Parses the given list of IP CIDR blocks into list of {@link IPRange}.
   */
  public static List<IPRange> parseIPRanges(List<String> ranges) {
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
      throw new IllegalArgumentException("Invalid zone. Zone must be in the format of <region>-<zone-name>");
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

  public static void emitMetric(ProvisionerContext context, String region, String  metricName) {
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
      try (Reader reader = new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8)) {
        return CharStreams.toString(reader);
      }
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  private DataprocUtils() {
    // no-op
  }
}
