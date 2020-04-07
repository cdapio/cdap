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
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class contains common methods that are needed by DataprocProvisioner and DataprocRuntimeJobManager.
 */
public final class DataprocUtils {
  public static final String CDAP_GCS_ROOT = "cdap-job";
  private static final Logger LOG = LoggerFactory.getLogger(DataprocUtils.class);
  private static final String GS_PREFIX = "gs://";

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

  private DataprocUtils() {
    // no-op
  }
}
