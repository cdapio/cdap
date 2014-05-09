package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.DistributedKeyManager;
import com.continuuity.security.auth.KeyIdentifier;
import com.continuuity.security.auth.KeyManager;
import com.continuuity.security.io.Codec;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import org.apache.twill.zookeeper.ZKClientService;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

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
