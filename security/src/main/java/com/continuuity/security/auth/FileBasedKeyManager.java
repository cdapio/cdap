package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.FileBasedSecurityModule;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.inject.Guice;
import com.google.inject.Injector;

import java.io.File;
import java.io.IOException;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 * Writes and loads a serialized secret key from file.
 */
public class FileBasedKeyManager extends AbstractKeyManager {
  private static final int KEY_VERSION = KeyIdentifier.Schemas.getVersion();
  private final String keyFilePath;
  private final Codec<KeyIdentifier> keyIdentifierCodec;

  /**
   * Create a new FileBasedKeyManager instance that persists keys in a local file.
   * @param conf
   */
  public FileBasedKeyManager(CConfiguration conf) {
    super(conf);
    this.keyFilePath = conf.get(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH);

    Injector injector = Guice.createInjector(new IOModule(), new FileBasedSecurityModule(), new ConfigModule());
    this.keyIdentifierCodec = injector.getInstance(KeyIdentifierCodec.class);
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
      Preconditions.checkState(keyFileDir.canWrite(),
                               "Configured keyFile directory " + keyFileDirectory + " exists but is not writable!");
    }

    // Read existing key from file.
    if (keyFile.exists()) {
      KeyIdentifier storedKey = keyIdentifierCodec.decode(Files.toByteArray(keyFile));
      this.currentKey = storedKey;
      allKeys.put(storedKey.getKeyId(), storedKey.getKey());
    } else {
      // Create a new key and write to file.
      generateKey();
      keyFile.createNewFile();
      Files.write(keyIdentifierCodec.encode(currentKey), keyFile);
    }
  }
}
