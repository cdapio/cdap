package com.continuuity.security.auth;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.guice.ConfigModule;
import com.continuuity.common.guice.IOModule;
import com.continuuity.common.guice.ZKClientModule;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.security.guice.SecurityModule;
import com.continuuity.security.guice.SecurityModules;
import com.continuuity.security.io.Codec;
import com.continuuity.security.zookeeper.SharedResourceCache;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class DistributedKeyManagerTest extends TestTokenManager {
  private static final Logger LOG = LoggerFactory.getLogger(DistributedKeyManagerTest.class);
  private static MiniZooKeeperCluster zkCluster;
  private static String zkConnectString;
  private static Injector injector1;
  private static Injector injector2;

  @BeforeClass
  public static void setup() throws Exception {
    HBaseTestingUtility testUtil = new HBaseTestingUtility();
    zkCluster = testUtil.startMiniZKCluster();
    zkConnectString = testUtil.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM) + ":"
      + zkCluster.getClientPort();
    LOG.info("Running ZK cluster at " + zkConnectString);
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.Zookeeper.QUORUM, zkConnectString);
    injector1 = Guice.createInjector(new ConfigModule(cConf, testUtil.getConfiguration()), new IOModule(),
                                     new SecurityModules().getDistributedModules(), new ZKClientModule());
    injector2 = Guice.createInjector(new ConfigModule(cConf, testUtil.getConfiguration()), new IOModule(),
                                     new SecurityModules().getDistributedModules(), new ZKClientModule());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    zkCluster.shutdown();
  }

  @Test
  public void testKeyDistribution() throws Exception {
    ZKClientService zk1 = injector1.getInstance(ZKClientService.class);
    zk1.startAndWait();
    DistributedKeyManager manager1 =
      new DistributedKeyManager(injector1.getInstance(CConfiguration.class),
          injector1.getInstance(Key.get(new TypeLiteral<Codec<KeyIdentifier>>() {})),
          zk1,
          injector1.getInstance(Key.get(new TypeLiteral<SharedResourceCache<KeyIdentifier>>() {})));

    ZKClientService zk2 = injector2.getInstance(ZKClientService.class);
    zk2.startAndWait();
    DistributedKeyManager manager2 =
      new DistributedKeyManager(injector2.getInstance(CConfiguration.class),
                                injector2.getInstance(Key.get(new TypeLiteral<Codec<KeyIdentifier>>() {})),
                                zk2,
                                injector2.getInstance(Key.get(new TypeLiteral<SharedResourceCache<KeyIdentifier>>() {})));

    manager1.setLeader(true);
    manager1.init();
    manager2.init();
    TimeUnit.MILLISECONDS.sleep(1000);

    TokenManager tokenManager1 = new TokenManager(manager1, injector1.getInstance(AccessTokenIdentifierCodec.class));
    TokenManager tokenManager2 = new TokenManager(manager2, injector2.getInstance(AccessTokenIdentifierCodec.class));

    long now = System.currentTimeMillis();
    AccessTokenIdentifier ident1 = new AccessTokenIdentifier("testuser", Lists.newArrayList("users", "admins"),
                                                             now, now + 60 * 60 * 1000);
    AccessToken token1 = tokenManager1.signIdentifier(ident1);
    // the second token manager should now have the secret key required to validate the signature
    tokenManager2.validateSecret(token1);
    AccessToken token2 = tokenManager2.signIdentifier(ident1);
    tokenManager1.validateSecret(token2);
    assertEquals(token1.getIdentifier().getUsername(), token2.getIdentifier().getUsername());
    assertEquals(token1.getIdentifier().getGroups(), token2.getIdentifier().getGroups());
    assertEquals(token1, token2);
  }

  @Override
  protected ImmutablePair<TokenManager, Codec<AccessToken>> getTokenManagerAndCodec() {
    return new ImmutablePair<TokenManager, Codec<AccessToken>>(injector1.getInstance(TokenManager.class),
                                                               injector1.getInstance(AccessTokenCodec.class));
  }
}
