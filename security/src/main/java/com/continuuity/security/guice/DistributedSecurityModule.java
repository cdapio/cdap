package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.DistributedKeyManager;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyManager;
import com.continuuity.security.io.Codec;
import com.continuuity.security.zookeeper.SharedResourceCache;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

/**
 *
 */
public class DistributedSecurityModule extends SecurityModule {
  @Override
  protected void bindKeyManager(Binder binder) {
    binder.bind(KeyManager.class).toProvider(DistributedKeyManagerProvider.class);
  }

  private static final class DistributedKeyManagerProvider implements Provider<KeyManager> {
    private final CConfiguration cConf;
    private final Codec<KeyIdentifier> keyCodec;
    private final ZKClientService zkClient;
    private final SharedResourceCache<KeyIdentifier> keyCache;


    @Inject
    DistributedKeyManagerProvider(CConfiguration cConf, Codec<KeyIdentifier> codec, ZKClientService zkClient,
                                  SharedResourceCache<KeyIdentifier> keyCache) {
      this.cConf = cConf;
      this.keyCodec = codec;
      this.zkClient = zkClient;
      this.keyCache = keyCache;
    }

    @Override
    public KeyManager get() {
      zkClient.startAndWait();
      KeyManager keyManager = new DistributedKeyManager(cConf, keyCodec, zkClient, keyCache);
      try {
        keyManager.init();
      } catch (NoSuchAlgorithmException nsae) {
        throw Throwables.propagate(nsae);
      } catch (IOException ioe) {
        throw Throwables.propagate(ioe);
      }
      return keyManager;
    }
  }
}
