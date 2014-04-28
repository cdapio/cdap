package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.security.io.Codec;
import com.continuuity.security.zookeeper.ResourceListener;
import com.continuuity.security.zookeeper.SharedResourceCache;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 */
public class DistributedKeyManager extends AbstractKeyManager implements ResourceListener<KeyIdentifier> {
  /**
   * Default execution frequency for the key update thread.  This is normally set much lower than the key expiration
   * interval to keep rotations happening at approximately the set frequency.
   */
  private static final long KEY_UPDATE_FREQUENCY = 60 * 1000;
  private static final Logger LOG = LoggerFactory.getLogger(DistributedKeyManager.class);
  private Timer timer;
  private long lastKeyUpdate;
  private boolean leader;

  public DistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClientService zookeeper,
                               SharedResourceCache<KeyIdentifier> keyCache) {
    super(conf);
    this.leader = conf.getBoolean(Constants.Security.DIST_KEY_MANAGER_LEADER, false);
    this.keyExpirationPeriod = conf.getLong(Constants.Security.TOKEN_DIGEST_KEY_EXPIRATION,
                                            Constants.Security.DEFAULT_TOKEN_DIGEST_KEY_EXPIRATION);
    keyCache.addListener(this);
    this.allKeys = keyCache;
  }

  @Override
  protected void doInit() throws IOException {
    LOG.info("Starting distributed key manager as " + (leader ? "leader" : "follower"));
    ((Service) this.allKeys).startAndWait();
    if (isLeader()) {
      rotateKey();
    }
    startExpirationThread();
  }

  public boolean isLeader() {
    return leader;
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
        if (leader) {
          long now = System.currentTimeMillis();
          if (lastKeyUpdate < (now - keyExpirationPeriod)) {
            rotateKey();
          }
        }
      }
    }, 0, Math.min(keyExpirationPeriod, KEY_UPDATE_FREQUENCY));
  }

  @Override
  public synchronized void onUpdate() {
    LOG.info("SharedResourceCache triggered update on key: leader={}", leader);
    for (Map.Entry<String, KeyIdentifier> keyEntry : allKeys.entrySet()) {
      if (currentKey == null || keyEntry.getValue().getExpiration() > currentKey.getExpiration()) {
        currentKey = keyEntry.getValue();
        LOG.info("Set current key to {}", currentKey);
      }
    }
  }

  @Override
  public synchronized void onResourceUpdate(KeyIdentifier instance) {
    LOG.info("SharedResourceCache triggered update: leader={}, resource key={}", leader, instance);
    if (currentKey == null || instance.getExpiration() > currentKey.getExpiration()) {
      currentKey = instance;
      LOG.info("Set current key: leader={}, key={}", leader, currentKey);
    }
  }
}
