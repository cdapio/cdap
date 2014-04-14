package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.NoSuchAlgorithmException;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 * Writes and loads a serialized secret key from file.
 */
public class SharedKeyManager extends AbstractKeyManager {
  private static final int KEY_VERSION = 1;
  private final String keyFileDirectory;
  private final String keyFileName;

  /**
   * Create a new SharedKeyManager instance that persists keys in file.
   * @param conf
   */
  public SharedKeyManager(CConfiguration conf) {
    super(conf);
    this.keyFileDirectory = conf.get(Constants.Security.CFG_SHARED_KEYFILE_DIR);
    this.keyFileName = conf.get(Constants.Security.CFG_SHARED_KEYFILE_NAME);
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
      if (keyFile.canRead()) {
        FileInputStream fis = new FileInputStream(keyFile.getAbsoluteFile());
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

        // Version of keyfile. Currently unused, but will be used when we make changes to our key.
        int keyFileVersion = Integer.parseInt(reader.readLine());
        String serializedKey = reader.readLine();
        int keyId = Integer.parseInt(reader.readLine());

        if (KEY_VERSION != keyFileVersion) {
          throw new IOException("Current key version and key version on file are incompatible.");
        }

        byte[] decodedKey = Base64.decodeBase64(serializedKey);

        SecretKey originalKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, keyAlgo);
        this.currentKey = new KeyIdentifier(originalKey, keyId);
        allKeys.put(keyId, originalKey);

      } else {
        throw new IOException("Inadequate permissions to read keyfile.");
      }
    } else {
      // Create a new key and write to file.
      generateKey();

      keyFile.createNewFile();
      FileWriter fw = new FileWriter(keyFile.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);

      String serializedKey = Base64.encodeBase64String(currentKey.getKey().getEncoded());
      bw.write(KEY_VERSION + "\n" + serializedKey + currentKey.getKeyId() + "\n");
      bw.close();
    }
  }
}
