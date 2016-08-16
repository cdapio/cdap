/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark;

import co.cask.cdap.common.io.LocationStatus;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.io.Processor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class is responsible for writing credentials to a location that Spark executor is expecting for
 * secure credentials update for long running process.
 */
public class SparkCredentialsUpdater extends AbstractIdleService implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(SparkCredentialsUpdater.class);

  // The following constants are copied from Spark because not all versions of Spark have them defined
  private static final String SPARK_YARN_CREDS_TEMP_EXTENSION = ".tmp";
  private static final String SPARK_YARN_CREDS_COUNTER_DELIM = "-";

  private final Supplier<Credentials> credentialsSupplier;
  private final Location credentialsDir;
  private final String fileNamePrefix;
  private final long updateIntervalMs;
  private final long cleanupExpireMs;
  private final int minFilesToKeep;
  private final Pattern fileNamePattern;
  private ScheduledExecutorService scheduler;
  private int generation;

  public SparkCredentialsUpdater(Supplier<Credentials> credentialsSupplier,
                                 Location credentialsDir, String fileNamePrefix, long updateIntervalMs,
                                 long cleanupExpireMs, int minFilesToKeep) {
    Preconditions.checkArgument(updateIntervalMs > 0, "Update interval must be positive");
    Preconditions.checkArgument(cleanupExpireMs > 0, "Cleanup expire time must be positive");
    Preconditions.checkArgument(minFilesToKeep >= 0, "Minimum files to keep must be non-zero");

    this.credentialsSupplier = credentialsSupplier;
    this.credentialsDir = credentialsDir;
    this.fileNamePrefix = fileNamePrefix;
    this.updateIntervalMs = updateIntervalMs;
    this.cleanupExpireMs = cleanupExpireMs;
    this.minFilesToKeep = minFilesToKeep;
    this.fileNamePattern = Pattern.compile(fileNamePrefix + SPARK_YARN_CREDS_COUNTER_DELIM + "(\\d+)");
  }

  @Override
  protected void startUp() throws Exception {
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("credentials-updater"));
    scheduler.submit(this).get();

    LOG.info("Credentials updater started");
  }

  @Override
  protected void shutDown() throws Exception {
    ExecutorService executor = scheduler;
    if (executor != null) {
      executor.shutdownNow();
    }

    LOG.info("Credentials updater stopped");
  }

  @Override
  public void run() {
    long nextUpdateTime = updateIntervalMs;
    try {
      if (generation == 0) {
        generation = findLatestGeneration();
      }

      // Write to the next generation file. It's ok to skip some generation if the write failed.
      generation++;

      Location credentialsFile = credentialsDir.append(fileNamePrefix + SPARK_YARN_CREDS_COUNTER_DELIM + generation);
      Location tempFile = credentialsDir.append(credentialsFile.getName() + SPARK_YARN_CREDS_TEMP_EXTENSION);

      // Writes the credentials to temp location, then rename to the final one
      Credentials credentials = credentialsSupplier.get();
      try (DataOutputStream os = new DataOutputStream(tempFile.getOutputStream("600"))) {
        credentials.writeTokenStorageToStream(os);
      }

      if (!credentialsFile.equals(tempFile.renameTo(credentialsFile))) {
        throw new IOException("Failed to rename from " + tempFile + " to " + credentialsFile);
      }

      LOG.info("Credentials written to {}", credentialsFile);

      // Schedule the next update.
      // Use the same logic as the Spark executor to calculate the update time.
      nextUpdateTime = getNextUpdateDelay(credentials);

      LOG.debug("Next credentials refresh at {}ms later", nextUpdateTime);
      scheduler.schedule(this, nextUpdateTime, TimeUnit.MILLISECONDS);
      cleanup();

    } catch (Exception e) {
      // Retry time is the min(1 minute, update interval)
      long retryDelay = Math.min(60000, nextUpdateTime);
      LOG.warn("Exception raised when saving credentials. Retry in {}ms", retryDelay, e);
      scheduler.schedule(this, retryDelay, TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  long getNextUpdateDelay(Credentials credentials) throws IOException {
    long now = System.currentTimeMillis();

    // This is almost the same logic as in SparkHadoopUtil.getTimeFromNowToRenewal
    for (Token<? extends TokenIdentifier> token : credentials.getAllTokens()) {
      if (DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(token.getKind())) {
        DelegationTokenIdentifier identifier = new DelegationTokenIdentifier();
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(token.getIdentifier()))) {
          identifier.readFields(input);

          // speed up by 2 seconds to account for any time race between driver and executor
          return Math.max(0L, (long) (identifier.getIssueDate() + 0.8 * updateIntervalMs) - now - 2000);
        }
      }
    }
    return 0L;
  }

  private void cleanup() {
    try {
      // Locations.processLocations is more efficient in getting the location last modified time
      Locations.processLocations(credentialsDir, false, new Processor<LocationStatus, Runnable>() {

        private final List<LocationStatus> cleanupFiles = new ArrayList<>();

        @Override
        public boolean process(LocationStatus status) {
          if (!status.isDir()) {
            cleanupFiles.add(status);
          }
          return true;
        }

        @Override
        public Runnable getResult() {
          return new Runnable() {
            @Override
            public void run() {
              if (cleanupFiles.size() <= minFilesToKeep) {
                return;
              }

              // Sort the list of files in descending order of last modified time.
              Collections.sort(cleanupFiles, new Comparator<LocationStatus>() {
                @Override
                public int compare(LocationStatus o1, LocationStatus o2) {
                  return Long.compare(o2.getLastModified(), o1.getLastModified());
                }
              });

              final long thresholdTime = System.currentTimeMillis() - cleanupExpireMs;

              // Remove the tail of the list, keep the top "minFilesToKeep"
              for (LocationStatus locationStatus : cleanupFiles.subList(minFilesToKeep, cleanupFiles.size())) {
                if (locationStatus.getLastModified() >= thresholdTime) {
                  continue;
                }

                Location location = credentialsDir.getLocationFactory().create(locationStatus.getUri());
                try {
                  location.delete();
                  LOG.debug("Removed old credential file {}", location);
                } catch (Exception e) {
                  LOG.warn("Failed to cleanup old credential file {}", location, e);
                }
              }
            }
          };
        }
      }).run();
    } catch (Exception e) {
      LOG.warn("Exception raised when cleaning up credential files in {}", credentialsDir, e);
    }
  }

  private int findLatestGeneration() throws IOException {
    int max = 0;

    for (Location location : credentialsDir.list()) {
      Matcher matcher = fileNamePattern.matcher(location.getName());
      if (matcher.matches()) {
        int generation = Integer.parseInt(matcher.group(1));
        if (generation > max) {
          max = generation;
        }
      }
    }

    return max;
  }
}
