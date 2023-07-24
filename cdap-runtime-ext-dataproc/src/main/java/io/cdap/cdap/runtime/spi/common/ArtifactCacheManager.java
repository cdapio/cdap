/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages the custom_time and Temporary Holds that need to be set/unset/updated on cached program
 * artifacts in GCS. Also maintains a usage counter of each of the cached artifacts in the same GCS
 * bucket.
 */
public class ArtifactCacheManager {

  private static final Gson GSON = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(ArtifactCacheManager.class);
  private static final SplittableRandom RANDOM = new SplittableRandom();

  private static final String CACHED_ARTIFACTS_USAGE_COUNT = "cached-artifacts-usage-count.json";
  private static final String CACHED_ARTIFACTS_LIST_FOR_RUNID = "cached-artifacts.json";
  private static final String CONTENT_TYPE_JSON = "application/json";
  private static final int MAX_RETRIES_FOR_CACHE_COUNTER_OPERATION = 10;
  private static final int MAX_RETRIES_TO_FETCH_CACHED_ARTIFACTS_FOR_RUN = 3;

  /**
   * Increases cache usage counter for all the files(cached artifacts) in the GCS bucket.
   */
  public void recordCacheUsageForArtifacts(Storage client, String bucket,
      Set<String> cachedArtifacts,
      String runRootPath, String cachedArtifactsPath, String runId) {
    try {
      String cachedArtifactsListFilePath = getPath(runRootPath, CACHED_ARTIFACTS_LIST_FOR_RUNID);
      storeCachedArtifactsForRun(client, bucket, cachedArtifacts, cachedArtifactsListFilePath);
      changeUsageCountForArtifacts(client, bucket, cachedArtifacts, cachedArtifactsPath, 1);
    } catch (Exception e) {
      LOG.warn("Unable to record artifacts cache usage for run: {}", runId, e);
    }
  }

  /**
   * Decreases cache usage counter for all the files(cached artifacts) in the GCS bucket. Also
   * updates custom_time and removes Temporary Hold if the artifact is not being used by any program
   * run.
   */
  public void releaseCacheUsageForArtifacts(Storage client, String bucket, String runRootPath,
      String runId) {
    try {
      String cachedArtifactsListFilePath = getPath(runRootPath, CACHED_ARTIFACTS_LIST_FOR_RUNID);
      Set<String> cachedArtifacts = getCachedArtifactsForRun(client, bucket,
          cachedArtifactsListFilePath);
      String cachedArtifactsPath = getPath(DataprocUtils.CDAP_GCS_ROOT,
          DataprocUtils.CDAP_CACHED_ARTIFACTS);
      changeUsageCountForArtifacts(client, bucket, cachedArtifacts, cachedArtifactsPath, -1);
    } catch (Exception e) {
      LOG.warn("Unable to release artifacts cache usage for run: {}", runId, e);
    }
  }

  private void changeUsageCountForArtifacts(Storage client, String bucket,
      Set<String> cachedArtifacts,
      String cachedArtifactsPath, int changeValue) throws InterruptedException {
    if (cachedArtifacts == null || cachedArtifacts.isEmpty()) {
      LOG.debug("No Cached Artifacts found for this run!");
      return;
    }
    String cacheCountFilePath = getPath(cachedArtifactsPath, CACHED_ARTIFACTS_USAGE_COUNT);
    BlobId blobId = BlobId.of(bucket, cacheCountFilePath);
    for (int i = 0; i < MAX_RETRIES_FOR_CACHE_COUNTER_OPERATION; i++) {
      try {
        Blob blob = client.get(blobId);
        if (blob == null || !blob.exists()) {
          LOG.debug("Creating ArtifactsCacheCounter {} at {}", CACHED_ARTIFACTS_USAGE_COUNT,
              cacheCountFilePath);
          blob = createCacheCounterFile(client, blobId);
        }
        Map<String, Integer> artifactCount = GSON.fromJson(
            new String(blob.getContent(), StandardCharsets.UTF_8),
            new TypeToken<Map<String, Integer>>() {
            }.getType());
        Set<String> artifactsCached = new HashSet<>(artifactCount.keySet());
        modifyCacheCounter(cachedArtifacts, cacheCountFilePath, artifactCount, changeValue);
        if (writeCacheCounterToGCS(blob, artifactCount)) {
          if (changeValue < 0) {
            updateCustomTimeAndHoldOnArtifacts(client, bucket, artifactCount, artifactsCached,
                cachedArtifactsPath);
          }
          break;
        }
      } catch (Exception e) {
        LOG.info("Exception while updating artifacts cache counter, retrying operation.", e);
      }
      Thread.sleep(RANDOM.nextInt(500));
    }
  }

  private void updateCustomTimeAndHoldOnArtifacts(Storage client, String bucket,
      Map<String, Integer> artifactCount,
      Set<String> artifactsCached, String cachedArtifactsPath) {
    artifactsCached.parallelStream().filter(artifact -> !artifactCount.containsKey(artifact))
        .forEach(artifact -> {
          String cachedArtifactFilePath = getPath(cachedArtifactsPath, artifact);
          try {
            DataprocUtils.removeTemporaryHoldOnGcsObject(client, bucket,
                BlobId.of(bucket, cachedArtifactFilePath),
                cachedArtifactFilePath);
          } catch (InterruptedException e) {
            LOG.warn("Unable to update custom time and temporary hold on cached artifacts", e);
          }
        });
  }

  private boolean writeCacheCounterToGCS(Blob blob, Map<String, Integer> artifactCount) {
    try (WritableByteChannel writer = blob.writer(Storage.BlobWriteOption.generationMatch())) {
      writer.write(ByteBuffer.wrap(GSON.toJson(artifactCount).getBytes(StandardCharsets.UTF_8)));
      writer.close();
      return true;
    } catch (IOException e) {
      LOG.warn("Exception while writing to artifacts cache counter file", e);
    }
    return false;
  }

  private void modifyCacheCounter(Set<String> cachedArtifacts, String cacheCountFilePath,
      Map<String, Integer> artifactCount, int changeValue) {
    for (String cachedArtifact : cachedArtifacts) {
      int newCount = artifactCount.getOrDefault(cachedArtifact, 0) + changeValue;
      if (newCount <= 0) {
        if (newCount < 0) {
          LOG.warn("Cache usage count less than 0 for {} in {}", cachedArtifact,
              cacheCountFilePath);
        }
        artifactCount.remove(cachedArtifact);
      } else {
        artifactCount.put(cachedArtifact, newCount);
      }
    }
  }

  private Blob createCacheCounterFile(Storage client, BlobId blobId) {
    try {
      BlobInfo createBlob = BlobInfo.newBuilder(blobId).setContentType(CONTENT_TYPE_JSON).build();
      client.create(createBlob, GSON.toJson(new HashMap<>()).getBytes(StandardCharsets.UTF_8),
          Storage.BlobTargetOption.doesNotExist());
    } catch (StorageException e) {
      if (e.getCode() != HttpURLConnection.HTTP_PRECON_FAILED) {
        throw e;
      }
    }
    return client.get(blobId);
  }

  private void storeCachedArtifactsForRun(Storage client, String bucket,
      Set<String> cachedArtifacts,
      String cachedArtifactsListFilePath) {
    BlobInfo createBlob = BlobInfo.newBuilder(bucket, cachedArtifactsListFilePath)
        .setContentType(CONTENT_TYPE_JSON).build();
    client.create(createBlob, GSON.toJson(cachedArtifacts).getBytes(StandardCharsets.UTF_8),
        Storage.BlobTargetOption.doesNotExist());
  }

  private Set<String> getCachedArtifactsForRun(Storage client, String bucket,
      String cachedArtifactsListFilePath) throws InterruptedException {
    BlobId blobId = BlobId.of(bucket, cachedArtifactsListFilePath);
    Set<String> files = null;
    for (int i = 0; i < MAX_RETRIES_TO_FETCH_CACHED_ARTIFACTS_FOR_RUN; i++) {
      try {
        Blob blob = client.get(blobId);
        if (blob == null || blob.getContent() == null) {
          continue;
        }
        files = GSON.fromJson(new String(blob.getContent(), StandardCharsets.UTF_8),
            new TypeToken<Set<String>>() {
            }.getType());
      } catch (StorageException e) {
        if (i == MAX_RETRIES_TO_FETCH_CACHED_ARTIFACTS_FOR_RUN - 1) {
          throw e;
        }
      }
      Thread.sleep(RANDOM.nextInt(500));
    }
    return files;
  }

  private String getPath(String... pathSubComponents) {
    return Joiner.on("/").join(pathSubComponents);
  }

}
