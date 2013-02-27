package com.continuuity.passport.server;

import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.http.handlers.AccountHandler;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import java.util.HashMap;
import java.util.Map;

/**
 * Mock Server to test out the Http endpoints.
 * Uses Mock implementations.
 */
public class MockServer {

  private final Server server;
  private int port ;

  public MockServer(int port) {
    this.port = port;
    server = new Server(port);
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addEventListener(new MockGuiceContextListener(new HashMap<String,String>()));
    context.addServlet(DefaultServlet.class, "/");
    context.addFilter(GuiceFilter.class, "/*", 0);
  }

  public void start() throws Exception {
    server.start();
  }

  public boolean isStarted() {
    return server.isStarted();
  }

  public void stop() throws Exception {
    server.stop();
  }

  public class MockGuiceContextListener extends GuiceServletContextListener {

    private final Map<String, String> config;
    private ServletContext servletContext;

    public MockGuiceContextListener(Map<String, String> config) {
      this.config = config;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
      this.servletContext = servletContextEvent.getServletContext();
      super.contextInitialized(servletContextEvent);
    }


    @Override
    protected Injector getInjector() {
      return Guice.createInjector(new JerseyServletModule() {
        @Override
      public void configureServlets(){
          MapBinder<String, String> configBinder = MapBinder.newMapBinder(binder(), String.class, String.class,
            Names.named("passport.config"));
          bind(AccountHandler.class);
          bind(DataManagementService.class).to(MockDataManagementServiceImpl.class);
          bind(AuthenticatorService.class).to(MockAuthenticatorImpl.class);
          bind(AccountDAO.class).to(MockAccountDAO.class);
          bind(VpcDAO.class).to(MockVPCDAO.class);
          bind(NonceDAO.class).to(MockNonceDAO.class);

          bind(GuiceContainer.class).asEagerSingleton();
          bind(DefaultServlet.class).asEagerSingleton();
          serve("/*").with(DefaultServlet.class);
          filter("/passport/*").through(GuiceContainer.class);

        }
      });

    }
    }


  }






