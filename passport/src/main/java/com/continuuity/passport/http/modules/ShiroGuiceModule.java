package com.continuuity.passport.http.modules;

import com.continuuity.passport.dal.db.JDBCAuthroizingRealm;
import org.apache.shiro.guice.ShiroModule;

/**
 * Shiro Guice bindings.
 */
public class ShiroGuiceModule extends ShiroModule {

  /**
   * Implement this method in order to configure your realms and any other Shiro customization you may need.
   */
 @Override
  protected void configureShiro() {
    bindRealm().to(JDBCAuthroizingRealm.class);
  }
}
