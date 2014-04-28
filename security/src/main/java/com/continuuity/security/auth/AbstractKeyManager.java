package com.continuuity.security.auth;


import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Random;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;


/**
 * AbstractKeyManager that provides the basic functionality that all key managers share. This includes
 * generation of keys and MACs, and validation of MACs. Subclasses are expected to override the init method.
 */
public abstract class AbstractKeyManager implements KeyManager {

  protected ThreadLocal<Mac> threadLocalMac;
  protected KeyGenerator keyGenerator;
  protected volatile KeyIdentifier currentKey;
  protected final Map<Integer, SecretKey> allKeys = Maps.newConcurrentMap();
  protected final String keyAlgo;
  protected final int keyLength;

  /**
   * An AbstractKeyManager that has common functionality of all keymanagers.
   * @param conf
   */
  public AbstractKeyManager(CConfiguration conf) {
    this(conf.get(Constants.Security.TOKEN_DIGEST_ALGO, Constants.Security.DEFAULT_TOKEN_DIGEST_ALGO),
         conf.getInt(Constants.Security.TOKEN_DIGEST_KEY_LENGTH, Constants.Security.DEFAULT_TOKEN_DIGEST_KEY_LENGTH));
  }

  public AbstractKeyManager(String keyAlgo, int keyLength) {
    this.keyAlgo = keyAlgo;
    this.keyLength = keyLength;
  }

  public final void init() throws NoSuchAlgorithmException, IOException {
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
    doInit();
  }

  /**
   * Extended classes must override this method to initialize/read the key(s) used for signing tokens.
   * @throws IOException
   */
  protected abstract void doInit() throws IOException;

  /**
   * Generates a new KeyIdentifier and sets that to be the current key being used.
   * @return A new KeyIdentifier.
   */
  protected final KeyIdentifier generateKey() {
    Random rand = new Random();
    int nextId;
    do {
      nextId = rand.nextInt(Integer.MAX_VALUE);
    } while(allKeys.containsKey(nextId));

    SecretKey nextKey = keyGenerator.generateKey();
    KeyIdentifier keyIdentifier = new KeyIdentifier(nextKey, nextId);
    allKeys.put(nextId, nextKey);
    this.currentKey = keyIdentifier;
    return keyIdentifier;
  }

  @Override
  public final <T> void validateMAC(Codec<T> codec, Signed<T> signedMessage)
    throws InvalidDigestException, InvalidKeyException {
    try {
      byte[] newDigest = generateMAC(signedMessage.getKeyId(), codec.encode(signedMessage.getMessage()));
      if (!Bytes.equals(signedMessage.getDigestBytes(), newDigest)) {
        throw new InvalidDigestException("Token signature is not valid!");
      }
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  @Override
  public final DigestId generateMAC(byte[] message) throws InvalidKeyException {
    KeyIdentifier signingKey = currentKey;
    byte[] digest = generateMAC(signingKey.getKey(), message);
    return new DigestId(signingKey.getKeyId(), digest);
  }

  /**
   * Computes a digest for the given input message, using the key identified by the given ID.
   * @param keyId Identifier of the secret key to use.
   * @param message The data over which we should generate a digest.
   * @return The computed digest.
   * @throws InvalidKeyException If the input {@code keyId} does not match a known key or the key is not accepted
   * by the internal {@code Mac} implementation.
   */
  protected final byte[] generateMAC(int keyId, byte[] message) throws InvalidKeyException {
    SecretKey key = allKeys.get(keyId);
    if (key == null) {
      throw new InvalidKeyException("No key found for ID " + keyId);
    }
    return generateMAC(key, message);
  }

  protected final byte[] generateMAC(SecretKey key, byte[] message) throws InvalidKeyException {
    Mac mac = threadLocalMac.get();
    // TODO: should we only initialize when the key changes?
    mac.init(key);
    return mac.doFinal(message);
  }
}
