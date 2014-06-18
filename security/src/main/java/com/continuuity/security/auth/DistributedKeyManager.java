package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.io.Codec;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.security.zookeeper.ResourceListener;
import com.continuuity.security.zookeeper.SharedResourceCache;
import com.google.common.base.Throwables;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link KeyManager} implementation that distributes shared secret keys via ZooKeeper to all instances, so that all
 * distributed instances maintain the same local cache of keys.  Instances of this class will perform leader election,
 * so that one instance functions as the "active" leader at a time.  The leader is responsible for periodically
 * generating a new secret key (with the frequency based on the configured value for
 * {@link Constants.Security#TOKEN_DIGEST_KEY_EXPIRATION}.  Prior keys are retained for as long as necessary to
 * ensure that any previously issued, non-expired tokens may be validated.  Once a previously used key's age exceeds
 * {@link Constants.Security#TOKEN_DIGEST_KEY_EXPIRATION} plus {@link Constants.Security#TOKEN_EXPIRATION},
 * the key can safely be removed.
 */
public class DistributedKeyManager extends AbstractKeyManager implements ResourceListener<KeyIdentifier> {
  /**
   * Default execution frequency for the key update thread.  This is normally set much lower than the key expiration
   * interval to keep rotations happening at approximately the set frequency.
   */
  private static final long KEY_UPDATE_FREQUENCY = 60 * 1000;
  private static final Logger LOG = LoggerFactory.getLogger(DistributedKeyManager.class);
  private SharedResourceCache<KeyIdentifier> keyCache;
  private Timer timer;
  private long lastKeyUpdate;
  protected final AtomicBoolean leader = new AtomicBoolean();
  private LeaderElection leaderElection;
  private String parentZNode;
  private ZKClient zookeeper;
  private final long tokenExpiration;

  public DistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClient zookeeper) {
    super(conf);
    this.parentZNode = conf.get(Constants.Security.DIST_KEY_PARENT_ZNODE);
    this.keyExpirationPeriod = conf.getLong(Constants.Security.TOKEN_DIGEST_KEY_EXPIRATION);
    this.tokenExpiration = conf.getLong(Constants.Security.TOKEN_EXPIRATION);
    this.zookeeper = ZKClients.namespace(zookeeper, parentZNode);
    this.keyCache = new SharedResourceCache<KeyIdentifier>(zookeeper, codec, "/keys");
  }

  @Override
  protected void doInit() throws IOException {
    this.keyCache.addListener(this);
    try {
      keyCache.init();
    } catch (InterruptedException ie) {
      throw Throwables.propagate(ie);
    }
    this.leaderElection = new LeaderElection(zookeeper, "/leader", new ElectionHandler() {
      @Override
      public void leader() {
        leader.set(true);
        LOG.info("Transitioned to leader");
        if (currentKey == null) {
          rotateKey();
        }
      }

      @Override
      public void follower() {
        leader.set(false);
        LOG.info("Transitioned to follower");
      }
    });
    this.leaderElection.start();
    startExpirationThread();
  }

  @Override
  public void shutDown() {
    leaderElection.stopAndWait();
  }

  @Override
  protected boolean hasKey(int id) {
    return keyCache.getIfPresent(Integer.toString(id)) != null;
  }

  @Override
  protected KeyIdentifier getKey(int id) {
    return keyCache.get(Integer.toString(id));
  }

  @Override
  protected void addKey(KeyIdentifier key) {
    keyCache.put(Integer.toString(key.getKeyId()), key);
  }


  private synchronized void rotateKey() {
    long now = System.currentTimeMillis();
    // create a new secret key
    generateKey();
    // clear out any expired keys
    for (KeyIdentifier keyIdent : keyCache.getResources()) {
      // we can only remove keys that expired prior to the oldest non-expired token
      if (keyIdent.getExpiration() < (now - tokenExpiration)) {
        LOG.info("Removing expired key: id={}, expiration={}", keyIdent.getKeyId(), keyIdent.getExpiration());
        keyCache.remove(Integer.toString(keyIdent.getKeyId()));
      }
    }
    lastKeyUpdate = now;
  }

  private void startExpirationThread() {
    timer = new Timer("DistributedKeyManager.key-rotator", true);
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        if (leader.get()) {
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
    LOG.debug("SharedResourceCache triggered update on key: leader={}", leader);
    for (KeyIdentifier keyEntry : keyCache.getResources()) {
      if (currentKey == null || keyEntry.getExpiration() > currentKey.getExpiration()) {
        currentKey = keyEntry;
        LOG.info("Set current key: leader={}, key={}", leader, currentKey.getKeyId());
      }
    }
  }

  @Override
  public synchronized void onResourceUpdate(String name, KeyIdentifier instance) {
    LOG.debug("SharedResourceCache triggered update: leader={}, resource key={}", leader, name);
    if (currentKey == null || instance.getExpiration() > currentKey.getExpiration()) {
      currentKey = instance;
      LOG.info("Set current key: leader={}, key={}", leader, currentKey.getKeyId());
    }
  }

  @Override
  public void onResourceDelete(String name) {
    LOG.info("Removed key: leader={}, key={}", leader, name);
  }

  @Override
  public void onError(String name, Throwable throwable) {
    /*
     * TODO: we may want to shutdown the server here, though for followers, staying up and processing requests
     * that we can may be more important.
     */
  }
}
