package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.security.io.Codec;
import com.continuuity.security.zookeeper.SharedResourceCache;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.zookeeper.ZKClient;

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 */
public class DistributedKeyManager extends AbstractKeyManager {
  /**
   * Default execution frequency for the key update thread.  This is normally set much lower than the key expiration
   * interval to keep rotations happening at approximately the set frequency.
   */
  private static final long KEY_UPDATE_FREQUENCY = 60 * 1000;
  private Timer timer;
  private long lastKeyUpdate;

  @Inject
  public DistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClient zookeeper) {
    super(conf);
    this.keyExpirationPeriod = conf.getLong(Constants.Security.TOKEN_DIGEST_KEY_EXPIRATION,
                                            Constants.Security.DEFAULT_TOKEN_DIGEST_KEY_EXPIRATION);
    this.allKeys = new SharedResourceCache<KeyIdentifier>(zookeeper, codec, "keys");
  }

  @Override
  protected void doInit() throws IOException {
    ((Service) this.allKeys).startAndWait();
    rotateKey();
    startExpirationThread();
  }

  private synchronized void rotateKey() {
    long now = System.currentTimeMillis();
    // create a new secret key
    generateKey();
    // clear out any expired keys
    for (Map.Entry<String, KeyIdentifier> entry : allKeys.entrySet()) {
      KeyIdentifier keyIdent = entry.getValue();
      if (keyIdent.getExpiration() < now) {
        allKeys.remove(entry.getKey());
      }
    }
    lastKeyUpdate = now;
  }

  private void startExpirationThread() {
    timer = new Timer("DistributedKeyManager.key-rotator", true);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        long now = System.currentTimeMillis();
        if (lastKeyUpdate < (now - keyExpirationPeriod)) {
          rotateKey();
        }
      }
    }, 0, Math.min(keyExpirationPeriod, KEY_UPDATE_FREQUENCY));
  }
}
