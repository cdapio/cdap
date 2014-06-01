package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.io.Codec;
import com.continuuity.security.auth.DistributedKeyManager;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyManager;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import org.apache.twill.zookeeper.ZKClientService;

/**
 * Configures dependency injection with all security class implementations required to run in a distributed
 * environment.
 */
public class DistributedSecurityModule extends SecurityModule {
  @Override
  protected void bindKeyManager(Binder binder) {
    binder.bind(KeyManager.class).toProvider(DistributedKeyManagerProvider.class).in(Scopes.SINGLETON);
  }

  private static final class DistributedKeyManagerProvider implements Provider<KeyManager> {
    private final CConfiguration cConf;
    private final Codec<KeyIdentifier> keyCodec;
    private final ZKClientService zkClient;


    @Inject
    DistributedKeyManagerProvider(CConfiguration cConf, Codec<KeyIdentifier> codec, ZKClientService zkClient) {
      this.cConf = cConf;
      this.keyCodec = codec;
      this.zkClient = zkClient;
    }

    @Override
    public KeyManager get() {
      return new DistributedKeyManager(cConf, keyCodec, zkClient);
    }
  }
}
