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

package co.cask.cdap.security.auth;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Codec;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 * Writes and loads a serialized secret key from file.
 */
public class FileBasedKeyManager extends MapBackedKeyManager {
  private final String keyFilePath;

  private final Codec<KeyIdentifier> keyIdentifierCodec;

  /**
   * Create a new FileBasedKeyManager instance that persists keys in a local file.
   * @param conf
   */
  public FileBasedKeyManager(CConfiguration conf, Codec<KeyIdentifier> keyIdentifierCodec) {
    super(conf);
    this.keyFilePath = conf.get(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH);
    this.keyIdentifierCodec = keyIdentifierCodec;
  }

  @Override
  public void doInit() throws IOException {
    File keyFile = new File(keyFilePath);
    String keyFileDirectory = keyFile.getParent();
    File keyFileDir = new File(keyFileDirectory);

    // Create directory for keyfile if it doesn't exist already.
    if (!keyFileDir.exists() && !keyFileDir.mkdir()) {
      throw new IOException("Failed to create directory " + keyFileDirectory + " for keyfile storage.");
    } else {
      Preconditions.checkState(keyFileDir.isDirectory(),
                               "Configured keyFile directory " + keyFileDirectory + " is not a directory!");
      Preconditions.checkState(keyFileDir.canRead(),
                               "Configured keyFile directory " + keyFileDirectory + " exists but is not readable!");
    }

    // Read existing key from file.
    if (keyFile.exists()) {
      KeyIdentifier storedKey = keyIdentifierCodec.decode(Files.toByteArray(keyFile));
      this.currentKey = storedKey;
      // the file-based key is considered valid forever
      allKeys.put(storedKey.getKeyId(), storedKey);
    } else {
      Preconditions.checkState(keyFileDir.canWrite(),
                               "Configured keyFile directory " + keyFileDirectory + " exists but is not writable!");
      // Create a new key and write to file.
      generateKey();
      keyFile.createNewFile();
      Files.write(keyIdentifierCodec.encode(currentKey), keyFile);
    }
  }


  @Override
  public void shutDown() {
    // nothing to do
  }
}
