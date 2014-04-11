package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.binary.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
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
 * Maintains secret keys used to sign and validate authentication tokens. Writes keys to file for persistence.
 */
public class SharedKeyManager extends AbstractKeyManager {
  private static String keyFileDirectory;
  private static String keyFileName;
  private static int keyFileVersion;

  public SharedKeyManager(CConfiguration conf) {
    super(conf);
    this.keyFileDirectory = conf.get(Constants.Security.SHARED_KEYFILE_DIR);
    this.keyFileName = conf.get(Constants.Security.SHARED_KEYFILE_NAME);
    this.keyFileVersion = Integer.parseInt(conf.get(Constants.Security.SHARED_KEYFILE_VERSION));
  }

  public void init() throws NoSuchAlgorithmException, IOException {
    keyGenerator = KeyGenerator.getInstance(keyAlgo);
    keyGenerator.init(keyLength);

    threadLocalMac = new ThreadLocal<Mac>() {
      @Override
      public Mac initialValue() {
        try {
          return Mac.getInstance(keyAlgo);
        } catch (NoSuchAlgorithmException nsae) {
          throw new IllegalArgumentException("Unknown algorithm for secret keys: " + keyAlgo);
        }
      }
    };

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
      Preconditions.checkState(keyFileDir.canWrite(),
                               "Configured keyFile directory " + keyFileDirectory + " exists but is not writable!");
    }

    File keyFile = new File(keyFileDir, keyFileName);

    // Read existing key from file.
    if (keyFile.exists()) {
      FileInputStream fis = new FileInputStream(keyFile.getAbsoluteFile());
      BufferedReader reader = new BufferedReader(new InputStreamReader(fis));

      // Version of keyfile. Currently unused, but will be used when we make changes to our key.
      int keyFileVersion = Integer.parseInt(reader.readLine());
      String serializedKey = reader.readLine();
      int keyId = Integer.parseInt(reader.readLine());

      byte[] decodedKey = Base64.decodeBase64(serializedKey);

      SecretKey originalKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, keyAlgo);
      this.currentKey = new KeyIdentifier(originalKey, keyId);
      allKeys.put(keyId, originalKey);
    } else {
      // Create a new key and write to file.
      generateKey();

      keyFile.createNewFile();
      FileWriter fw = new FileWriter(keyFile.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);

      String serializedKey = Base64.encodeBase64String(currentKey.getKey().getEncoded());
      bw.write(keyFileVersion + "\n" + serializedKey + currentKey.getKeyId() + "\n");
      bw.close();
    }
  }
}
