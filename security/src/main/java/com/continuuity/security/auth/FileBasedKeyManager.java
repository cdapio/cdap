package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Codec;
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
