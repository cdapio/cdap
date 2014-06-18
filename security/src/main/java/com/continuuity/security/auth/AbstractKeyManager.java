package com.continuuity.security.auth;


import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Codec;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Random;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;


/**
 * AbstractKeyManager that provides the basic functionality that all key managers share. This includes
 * generation of keys and MACs, and validation of MACs. Subclasses are expected to override the init method.
 */
public abstract class AbstractKeyManager extends AbstractIdleService implements KeyManager {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractKeyManager.class);

  protected ThreadLocal<Mac> threadLocalMac;
  protected KeyGenerator keyGenerator;
  protected volatile KeyIdentifier currentKey;
  protected final String keyAlgo;
  protected final int keyLength;
  /**
   * Time duration (in milliseconds) after which an active secret key should be retired. A value or zero or less
   * means no expiration.
   */
  protected long keyExpirationPeriod = 0;


  /**
   * An AbstractKeyManager that has common functionality of all keymanagers.
   * @param conf
   */
  public AbstractKeyManager(CConfiguration conf) {
    this(conf.get(Constants.Security.TOKEN_DIGEST_ALGO),
         conf.getInt(Constants.Security.TOKEN_DIGEST_KEY_LENGTH));
  }

  public AbstractKeyManager(String keyAlgo, int keyLength) {
    this.keyAlgo = keyAlgo;
    this.keyLength = keyLength;
  }

  @Override
  public final void startUp() throws NoSuchAlgorithmException, IOException {
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
   * Returns whether or not a key exists for the given unique ID.
   */
  protected abstract boolean hasKey(int id);

  /**
   * Returns the key instance matching a given unique ID.
   */
  protected abstract KeyIdentifier getKey(int id);

  /**
   * Adds a given key instance.
   */
  protected abstract void addKey(KeyIdentifier key);

  /**
   * Generates a new KeyIdentifier and sets that to be the current key being used.
   * @return A new KeyIdentifier.
   */
  protected final KeyIdentifier generateKey() {
    Random rand = new Random();
    int nextId;
    do {
      nextId = rand.nextInt(Integer.MAX_VALUE);
    } while (hasKey(nextId));

    long now = System.currentTimeMillis();
    SecretKey nextKey = keyGenerator.generateKey();
    KeyIdentifier keyIdentifier =
      new KeyIdentifier(nextKey, nextId, keyExpirationPeriod > 0 ? (now + keyExpirationPeriod) : Long.MAX_VALUE);
    addKey(keyIdentifier);
    this.currentKey = keyIdentifier;
    LOG.info("Changed current key to {}", currentKey);
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
    KeyIdentifier key = getKey(keyId);
    if (key == null) {
      throw new InvalidKeyException("No key found for ID " + keyId);
    }
    return generateMAC(key.getKey(), message);
  }

  protected final byte[] generateMAC(SecretKey key, byte[] message) throws InvalidKeyException {
    Mac mac = threadLocalMac.get();
    // TODO: should we only initialize when the key changes?
    mac.init(key);
    return mac.doFinal(message);
  }
}
