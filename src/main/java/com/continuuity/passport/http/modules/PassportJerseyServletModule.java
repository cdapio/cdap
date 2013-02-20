package com.continuuity.passport.http.modules;

import com.continuuity.passport.core.service.Authenticator;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.dal.db.AccountDBAccess;
import com.continuuity.passport.dal.db.NonceDBAccess;
import com.continuuity.passport.dal.db.VpcDBAccess;
import com.continuuity.passport.http.handlers.AccountHandler;
import com.continuuity.passport.http.handlers.ActivationHandler;
import com.continuuity.passport.http.handlers.NonceHandler;
import com.continuuity.passport.http.handlers.VPCHandler;
import com.continuuity.passport.impl.AuthenticatorImpl;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.mortbay.jetty.servlet.DefaultServlet;

import java.util.Map;

/**
 *
 */
public class PassportJerseyServletModule extends JerseyServletModule {

  private final Map<String, String> config;

  public PassportJerseyServletModule(Map<String, String> config) {
    this.config = config;
  }

  @Override
  protected void configureServlets() {
    bindings();
    filters();
  }

  private void bindings() {


    MapBinder<String, String> configBinder = MapBinder.newMapBinder(binder(), String.class, String.class, Names.named("passport.config"));
    for (Map.Entry<String, String> entry : config.entrySet()) {
      configBinder.addBinding(entry.getKey()).toInstance(entry.getValue());
    }

    //Bind ReST resources
    bind(AccountHandler.class);
    bind(ActivationHandler.class);
    bind(NonceHandler.class);
    bind(VPCHandler.class);

    //Bind DataManagementService and Authenticator to default implementations
    bind(DataManagementService.class).to(DataManagementServiceImpl.class);
    bind(Authenticator.class).to(AuthenticatorImpl.class);

    //Bind Data Access objects
    bind(AccountDAO.class).to(AccountDBAccess.class);
    bind(VpcDAO.class).to(VpcDBAccess.class);
    bind(NonceDAO.class).to(NonceDBAccess.class);


    bind(GuiceContainer.class).asEagerSingleton();
    bind(DefaultServlet.class).asEagerSingleton();
    serve("/*").with(DefaultServlet.class);

  }

  private void filters() {
    filter("/passport/*").through(GuiceContainer.class);
    //TODO: Add shiro filters
  }
}
