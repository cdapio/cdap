package com.continuuity.security.auth;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.collect.Maps;
import com.google.inject.Inject;

import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Maintains secret keys used to sign and validate authentication tokens.
 */
public class KeyManager {
  /**
   * Represents the combination of a digest computed on a mesage using a secret key, and the ID of the secret key
   * used to compute the digest.  Both elements are needed in order to later recompute (validate) the digest.
   */
  public static class DigestId {
    private final int id;
    private final byte[] digest;

    public DigestId(int id, byte[] digest) {
      this.id = id;
      this.digest = digest;
    }

    public int getId() {
      return id;
    }

    public byte[] getDigest() {
      return digest;
    }
  }

  /**
   * Represents a secret key to use for message signing, plus a unique sequence number identifying it.
   */
  private static class KeyIdentifier {
    private final SecretKey key;
    private final int keyId;

    public KeyIdentifier(SecretKey key, int id) {
      this.key = key;
      this.keyId = id;
    }

    public SecretKey getKey() {
      return key;
    }

    public int getKeyId() {
      return keyId;
    }
  }

  private ThreadLocal<Mac> threadLocalMac;

  private KeyGenerator keyGenerator;
  private volatile KeyIdentifier currentKey;
  private AtomicInteger keySequence = new AtomicInteger();
  private Map<Integer, SecretKey> allKeys = Maps.newConcurrentMap();
  private String keyAlgo;
  private int keyLength;

  public KeyManager() {
    this(Constants.Security.TOKEN_DIGEST_ALGO, Constants.Security.DEFAULT_TOKEN_DIGEST_KEY_LENGTH);
  }

  @Inject
  public KeyManager(CConfiguration conf) {
    this(conf.get(Constants.Security.TOKEN_DIGEST_ALGO, Constants.Security.DEFAULT_TOKEN_DIGEST_ALGO),
         conf.getInt(Constants.Security.TOKEN_DIGEST_KEY_LENGTH, Constants.Security.DEFAULT_TOKEN_DIGEST_KEY_LENGTH));
  }

  public KeyManager(String keyAlgo, int keyLength) {
    this.keyAlgo = keyAlgo;
    this.keyLength = keyLength;
  }

  public void init() throws NoSuchAlgorithmException {
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

    rotateKey();
  }

  protected void rotateKey() {
    int nextId = keySequence.incrementAndGet();
    SecretKey nextKey = keyGenerator.generateKey();
    KeyIdentifier keyIdentifier = new KeyIdentifier(nextKey, nextId);
    allKeys.put(nextId, nextKey);
    this.currentKey = keyIdentifier;
  }

  /**
   * Computes a digest for the given input message, using the current secret key.
   * @param message The data over which we should generate a digest.
   * @return The computed digest and the ID of the secret key used in generation.
   * @throws InvalidKeyException If the internal {@code Mac} implementation does not accept the given key.
   */
  public DigestId generateMAC(byte[] message) throws InvalidKeyException {
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
  public byte[] generateMAC(int keyId, byte[] message) throws InvalidKeyException {
    SecretKey key = allKeys.get(keyId);
    if (key == null) {
      throw new InvalidKeyException("No key found for ID " + keyId);
    }
    return generateMAC(key, message);
  }

  /**
   * Recomputes the digest for the given message and verifies that it matches the provided value.
   * @param signedMessage The message and digest to validate.
   */
  public void validateMAC(Signed signedMessage) throws InvalidDigestException, InvalidKeyException {
    byte[] newDigest = generateMAC(signedMessage.getKeyId(), signedMessage.getMessageBytes());
    if (!Bytes.equals(signedMessage.getDigestBytes(), newDigest)) {
      throw new InvalidDigestException("Token signature is not valid!");
    }
  }

  private byte[] generateMAC(SecretKey key, byte[] message) throws InvalidKeyException {
    Mac mac = threadLocalMac.get();
    // TODO: should we only initialize when the key changes?
    mac.init(key);
    return mac.doFinal(message);
  }
}
