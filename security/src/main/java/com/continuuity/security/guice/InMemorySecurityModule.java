package com.continuuity.security.guice;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.security.auth.InMemoryKeyManager;
import com.continuuity.security.auth.KeyManager;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.Provider;

import java.security.NoSuchAlgorithmException;

/**
 *
 */
public class InMemorySecurityModule extends SecurityModule {

  @Override
  protected Provider<KeyManager> getKeyManagerProvider() {

     class InMemoryKeyManagerProvider implements Provider<KeyManager> {
       private CConfiguration cConf;

       @Inject(optional = true)
       public void setCConfiguration(CConfiguration conf) {
         this.cConf = conf;
       }

       @Override
       public KeyManager get() {
         this.cConf = CConfiguration.create();
         InMemoryKeyManager keyManager = new InMemoryKeyManager(this.cConf);
         try {
           keyManager.init();
         } catch (NoSuchAlgorithmException nsae) {
           throw Throwables.propagate(nsae);
         }
         return keyManager;
       }
     }
    return new InMemoryKeyManagerProvider();
  }


}
