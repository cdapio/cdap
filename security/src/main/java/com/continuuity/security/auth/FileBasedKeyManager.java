package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.security.guice.FileBasedSecurityModule;
import com.google.common.base.Preconditions;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 * Writes and loads a serialized secret key from file.
 */
public class FileBasedKeyManager extends AbstractKeyManager {
  private static final int KEY_VERSION = KeyIdentifier.Schemas.getVersion();
  private final String keyFileDirectory;
  private final String keyFileName;
  private final Codec<KeyIdentifier> keyIdentifierCodec;

  /**
   * Create a new FileBasedKeyManager instance that persists keys in a local file.
   * @param conf
   */
  public FileBasedKeyManager(CConfiguration conf) {
    super(conf);
    this.keyFileDirectory = conf.get(Constants.Security.CFG_FILE_BASED_KEYFILE_DIR);
    this.keyFileName = conf.get(Constants.Security.CFG_FILE_BASED_KEYFILE_NAME);

    Injector injector = Guice.createInjector(new IOModule(), new FileBasedSecurityModule(), new ConfigModule());
    this.keyIdentifierCodec = injector.getInstance(KeyIdentifierCodec.class);
  }

  @Override
  public void init() throws NoSuchAlgorithmException, IOException {
    super.init();

    File keyFileDir = new File(keyFileDirectory);
    // Create directory for keyfile if it doesn't exist already.
    if (!keyFileDir.exists()) {
      if (!keyFileDir.mkdirs()) {
        throw new IOException("Failed to create directory " + keyFileDirectory +
                                " for keyfile storage.");
      }
    } else {
      Preconditions.checkState(keyFileDir.isDirectory(),
                               "Configured keyFile directory " + keyFileDirectory + " is not a directory!");
      Preconditions.checkState(keyFileDir.canRead(),
                               "Configured keyFile directory " + keyFileDirectory + " exists but is not readable!");
      Preconditions.checkState(keyFileDir.canWrite(),
                               "Configured keyFile directory " + keyFileDirectory + " exists but is not writable!");
    }

    File keyFile = new File(keyFileDir, keyFileName);

    // Read existing key from file.
    if (keyFile.exists()) {
        FileInputStream input = new FileInputStream(keyFile.getAbsoluteFile());
        try {
          KeyIdentifier storedKey = keyIdentifierCodec.decode(IOUtils.toByteArray(input));
          this.currentKey = storedKey;
          allKeys.put(storedKey.getKeyId(), storedKey.getKey());
        } catch (IOException ioe) {
          throw ioe;
        } finally {
          input.close();
        }
    } else {
      // Create a new key and write to file.
      generateKey();
      keyFile.createNewFile();
      FileOutputStream output = new FileOutputStream(keyFile.getAbsoluteFile());
      try {
        IOUtils.write(keyIdentifierCodec.encode(currentKey), output);
      } catch (IOException ioe) {
        throw ioe;
      } finally {
        output.close();
      }
    }
  }
}
