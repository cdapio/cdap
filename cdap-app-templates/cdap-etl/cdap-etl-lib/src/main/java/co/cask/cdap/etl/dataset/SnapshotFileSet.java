/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.etl.dataset;

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.etl.common.SnapshotFileSetConfig;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.twill.filesystem.Location;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Writes snapshots as partitions of a PartitionedFileSet, and keeps track of which partition is the most recent
 * partition. Also used to read the latest snapshot.
 *
 * Note: this should be a CDAP dataset, but plugins are not able to add custom datasets until CDAP-3992 is fixed.
 *       it should also implement DatasetOutputCommitter.
 */
public class SnapshotFileSet {
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final Gson GSON = new Gson();
  private static final String STATE_FILE_NAME = "state";
  private final PartitionedFileSet files;

  public SnapshotFileSet(PartitionedFileSet files) {
    this.files = files;
  }

  public static PartitionedFileSetProperties.Builder getBaseProperties(SnapshotFileSetConfig config) {
    PartitionedFileSetProperties.Builder propertiesBuilder = PartitionedFileSetProperties.builder()
      .setPartitioning(Partitioning.builder().addLongField("snapshot").build());

    if (!Strings.isNullOrEmpty(config.getBasePath())) {
      propertiesBuilder.setBasePath(config.getBasePath());
    }

    if (config.getProperties() != null) {
      try {
        Map<String, String> properties = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
        if (properties != null) {
          propertiesBuilder.addAll(properties);
        }
      } catch (Exception e) {
        throw new IllegalArgumentException("Could not decode the 'properties' setting. Please check that it " +
          "is a JSON Object of string to string. Failed with error: " + e.getMessage(), e);
      }
    }

    return propertiesBuilder;
  }

  @Nullable
  public Location getLocation() throws IOException, InterruptedException {
    Location lock = lock();
    try {
      PartitionDetail partitionDetail = getLatestPartition();
      if (partitionDetail == null) {
        return null;
      }
      return partitionDetail.getLocation();
    } finally {
      lock.delete();
    }
  }

  public void onSuccess(long snapshotTime) throws IOException, InterruptedException {
    Location lock = lock();
    try {
      // add partition
      PartitionKey partitionKey = PartitionKey.builder().addLongField("snapshot", snapshotTime).build();
      files.addPartition(partitionKey, String.valueOf(snapshotTime));

      // update state file that contains the latest snapshot
      Long latestSnapshot = getLatestSnapshot();
      if (latestSnapshot == null || snapshotTime > latestSnapshot) {
        Location stateFile = files.getEmbeddedFileSet().getBaseLocation().append(STATE_FILE_NAME);
        stateFile.delete();
        try (OutputStream outputStream = stateFile.getOutputStream()) {
          outputStream.write(String.valueOf(snapshotTime).getBytes(Charsets.UTF_8));
        }
      }
    } finally {
      lock.delete();
    }
  }

  public Map<String, String> getOutputArguments(long snapshotTime, Map<String, String> otherProperties) {
    Map<String, String> args = new HashMap<>();
    args.putAll(otherProperties);

    PartitionKey outputKey = PartitionKey.builder().addLongField("snapshot", snapshotTime).build();
    PartitionedFileSetArguments.setOutputPartitionKey(args, outputKey);
    return args;
  }

  public Map<String, String> getInputArguments(Map<String, String> otherProperties)
    throws IOException, InterruptedException {

    Location lock = lock();
    try {
      PartitionDetail partition = getLatestPartition();
      if (partition == null) {
        throw new IllegalArgumentException("Snapshot fileset does not a latest snapshot, so cannot be read.");
      }
      Map<String, String> args = new HashMap<>();
      args.putAll(otherProperties);
      PartitionedFileSetArguments.addInputPartition(args, partition);
      return args;
    } finally {
      lock.delete();
    }
  }

  private PartitionDetail getLatestPartition() throws IOException {
    Long latestTime = getLatestSnapshot();
    if (latestTime == null) {
      return null;
    }

    PartitionKey partitionKey = PartitionKey.builder().addLongField("snapshot", latestTime).build();
    PartitionDetail partitionDetail = files.getPartition(partitionKey);

    if (partitionDetail == null) {
      throw new IllegalStateException(String.format("No snapshot files found for latest recorded snapshot from '%d'. " +
        "This can happen if files are deleted manually without updating the state file. " +
        "Please fix the state file to contain the latest snapshot, or delete the file and write another snapshot.",
        latestTime));
    }
    return partitionDetail;
  }

  // should only be called after lock()
  private Long getLatestSnapshot() throws IOException {
    Location stateFile = files.getEmbeddedFileSet().getBaseLocation().append(STATE_FILE_NAME);
    if (!stateFile.exists()) {
      return null;
    }

    try (InputStreamReader reader = new InputStreamReader(stateFile.getInputStream(), Charsets.UTF_8)) {
      String val = CharStreams.toString(reader);
      return Long.valueOf(val);
    }
  }

  private Location lock() throws IOException, InterruptedException {
    // create a lock file in case there is somebody updating the latest snapshot
    Location lockFile = files.getEmbeddedFileSet().getBaseLocation().append("lock");

    int retries = 0;
    int maxRetries = 20;
    while (!lockFile.createNew()) {
      if (retries > maxRetries) {
        throw new IOException("Failed to create lock file. If there is a file named 'lock' in the " +
          "base path, but there is nobody updating the latest snapshot, please delete the 'lock' file.");
      }

      TimeUnit.SECONDS.sleep(1);
      retries++;
    }
    return lockFile;
  }
}
