/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.utils.DirUtils;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 * Writes and loads a serialized secret key from file.
 */
public class FileBasedKeyManager extends MapBackedKeyManager {

  private static final Logger LOG = LoggerFactory.getLogger(FileBasedKeyManager.class);
  private static final long KEY_FILE_POLL_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(5);

  private final Path keyFile;
  private final Codec<KeyIdentifier> keyIdentifierCodec;
  private ScheduledExecutorService scheduler;
  private long keyFileLastModified;

  /**
   * Create a new FileBasedKeyManager instance that persists keys in a local file.
   */
  @Inject
  public FileBasedKeyManager(CConfiguration conf, Codec<KeyIdentifier> keyIdentifierCodec) {
    super(conf);
    this.keyFile = Paths.get(conf.get(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH));
    this.keyIdentifierCodec = keyIdentifierCodec;
  }

  @Override
  public void doInit() throws IOException {
    // Create directory for keyfile if it doesn't exist already.
    if (!DirUtils.mkdirs(keyFile.toFile().getParentFile())) {
      throw new IOException("Failed to create directory " + keyFile.getParent() + " for key file storage.");
    }

    reloadKeyFile();

    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("key-manager-watcher"));
    scheduler.scheduleWithFixedDelay(this::checkKeyFileChange,
                                     KEY_FILE_POLL_INTERVAL_MILLIS,
                                     KEY_FILE_POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
  }

  @Override
  public void shutDown() {
    if (scheduler != null) {
      scheduler.shutdownNow();
    }
  }

  /**
   * Reloads the key from the configured key file.
   */
  private void reloadKeyFile() throws IOException {
    if (Files.exists(keyFile)) {
      keyFileLastModified = Files.getLastModifiedTime(keyFile).toMillis();
      KeyIdentifier key = keyIdentifierCodec.decode(Files.readAllBytes(keyFile));
      this.currentKey = key;
      allKeys.put(key.getKeyId(), key);

      LOG.debug("Key {} read from file {}", key.getKeyId(), keyFile);
    } else {
      // Create a new key and write to the file.
      KeyIdentifier key = generateKey();
      Files.write(keyFile, keyIdentifierCodec.encode(key), StandardOpenOption.CREATE_NEW);
      // On creating new key, we leave the last modified time to 0. This will trigger
      // and extra reload key call when the file poller thread see the newer timestamp.

      LOG.debug("Generated key {} and written to file {}", key.getKeyId(), keyFile);
    }
  }

  /**
   * Watches for file system changes to the key file. On file change detected,
   * {@link #reloadKeyFile()} will be called.
   */
  private void checkKeyFileChange() {
    try {
      if (!Files.exists(keyFile) || keyFileLastModified != Files.getLastModifiedTime(keyFile).toMillis()) {
        reloadKeyFile();
      }
    } catch (Exception e) {
      LOG.warn("Failed to check and reload key file. Will be retried", e);
    }
  }
}
