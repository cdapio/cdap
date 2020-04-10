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

/**
 * This class contains common methods that are needed by DataprocProvisioner and DataprocRuntimeJobManager.
 */
public final class DataprocUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DataprocUtils.class);
  public static final String CDAP_GCS_ROOT = "cdap-job";

  /**
   * Deletes provided directory path on GCS.
   *
   * @param storageClient storage client
   * @param bucket bucket
   * @param path dir path to delete
   */
  public static void deleteGCSPath(Storage storageClient, String bucket, String path) {
    try {
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
    } catch (Exception e) {
      LOG.warn(String.format("GCS path %s was not cleaned up for bucket %s due to %s. ",
                             path, bucket, e.getMessage()), e);
    }
  }

  /**
   * Utilty class to parse the keyvalue string from UI Widget and return back HashMap
   * @param configValue
   * @param delimiter
   * @param keyValueDelimiter
   * @return
   */
  public static Map<String, String> parseKeyValueConfig(String configValue, String delimiter,
                                                        String keyValueDelimiter) {
    Map<String, String> map = new HashMap<>();
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter);
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
