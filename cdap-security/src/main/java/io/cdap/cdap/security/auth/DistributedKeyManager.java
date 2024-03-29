/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.security.auth;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import io.cdap.cdap.security.zookeeper.ResourceListener;
import io.cdap.cdap.security.zookeeper.SharedResourceCache;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.internal.zookeeper.LeaderElection;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link KeyManager} implementation that distributes shared secret keys via ZooKeeper to all
 * instances, so that all distributed instances maintain the same local cache of keys.  Instances of
 * this class will perform leader election, so that one instance functions as the "active" leader at
 * a time.  The leader is responsible for periodically generating a new secret key (with the
 * frequency based on the configured value for {@link Constants.Security#TOKEN_DIGEST_KEY_EXPIRATION}.
 * Prior keys are retained for as long as necessary to ensure that any previously issued,
 * non-expired tokens may be validated.  Once a previously used key's age exceeds {@link
 * Constants.Security#TOKEN_DIGEST_KEY_EXPIRATION} plus {@link Constants.Security#TOKEN_EXPIRATION},
 * the key can safely be removed.
 */
public class DistributedKeyManager extends AbstractKeyManager implements
    ResourceListener<KeyIdentifier> {

  /**
   * Default execution frequency for the key update thread.  This is normally set much lower than
   * the key expiration interval to keep rotations happening at approximately the set frequency.
   */
  private static final long KEY_UPDATE_FREQUENCY = 60 * 1000;
  private static final Logger LOG = LoggerFactory.getLogger(DistributedKeyManager.class);

  private final SharedResourceCache<KeyIdentifier> keyCache;

  private Timer timer;
  private long lastKeyUpdate;
  protected final AtomicBoolean leader = new AtomicBoolean();
  private LeaderElection leaderElection;
  private ZKClient zookeeper;
  private final long maxTokenExpiration;

  @Inject
  DistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClient zookeeper) {
    this(conf, codec, zookeeper, getACLs(conf));
  }

  public DistributedKeyManager(CConfiguration conf, Codec<KeyIdentifier> codec, ZKClient zookeeper,
      List<ACL> acls) {
    super(conf);
    String parentZNode = conf.get(Constants.Security.DIST_KEY_PARENT_ZNODE);
    this.keyExpirationPeriod = conf.getLong(Constants.Security.TOKEN_DIGEST_KEY_EXPIRATION);
    this.maxTokenExpiration = Math.max(
        conf.getLong(Constants.Security.EXTENDED_TOKEN_EXPIRATION),
        conf.getLong(Constants.Security.TOKEN_EXPIRATION));
    this.zookeeper = ZKClients.namespace(zookeeper, parentZNode);

    if (acls.isEmpty()) {
      LOG.warn("ZooKeeper ACL list is empty for keys!");
      acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    }
    LOG.debug("ZooKeeper ACLs {} for keys", acls);
    this.keyCache = new SharedResourceCache<>(zookeeper, codec, "/keys", acls);
  }

  @Override
  protected void doInit() {
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
        LOG.debug("Transitioned to leader");
        if (currentKey == null) {
          rotateKey();
        }
      }

      @Override
      public void follower() {
        leader.set(false);
        LOG.debug("Transitioned to follower");
      }
    });
    this.leaderElection.start();
    startExpirationThread();
  }

  @Override
  public void shutDown() {
    if (timer != null) {
      timer.cancel();
    }
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
      if (keyIdent.getExpiration() < (now - maxTokenExpiration)) {
        LOG.debug("Removing expired key: id={}, expiration={}", keyIdent.getKeyId(),
            keyIdent.getExpiration());
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
        LOG.debug("Set current key: leader={}, key={}", leader, currentKey.getKeyId());
      }
    }
  }

  @Override
  public synchronized void onResourceUpdate(String name, KeyIdentifier instance) {
    LOG.debug("SharedResourceCache triggered update: leader={}, resource key={}", leader, name);
    if (currentKey == null || instance.getExpiration() > currentKey.getExpiration()) {
      currentKey = instance;
      LOG.debug("Set current key: leader={}, key={}", leader, currentKey.getKeyId());
    }
  }

  @Override
  public void onResourceDelete(String name) {
    LOG.debug("Removed key: leader={}, key={}", leader, name);
  }

  @Override
  public void onError(String name, Throwable throwable) {
    /*
     * TODO: we may want to shutdown the server here, though for followers, staying up and processing requests
     * that we can may be more important.
     */
  }

  /**
   * Applies Zookeeper ACLs if Kerberos is enabled.
   *
   * @param cConf configuration object
   * @return Zookeeper ACLs
   */
  static List<ACL> getACLs(CConfiguration cConf) {
    if (SecurityUtil.isKerberosEnabled(cConf)) {
      return ZooDefs.Ids.CREATOR_ALL_ACL;
    }

    LOG.warn("Not adding ACLs on keys in ZooKeeper as Kerberos is not enabled");
    return ZooDefs.Ids.OPEN_ACL_UNSAFE;
  }
}
