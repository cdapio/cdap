package com.continuuity.passport.server;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.passport.Constants;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.ProfanityFilter;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.dal.db.AccountDBAccess;
import com.continuuity.passport.dal.db.NonceDBAccess;
import com.continuuity.passport.dal.db.ProfanityFilterFileAccess;
import com.continuuity.passport.dal.db.VpcDBAccess;
import com.continuuity.passport.http.handlers.AccountHandler;
import com.continuuity.passport.impl.AuthenticatorServiceImpl;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.sql.ConnectionPoolDataSource;

/**
 * Mock Server to test out the Http endpoints.
 * Uses Mock implementations.
 */
public class MockServer {

  private final Server server;
  private final int port;


  public MockServer(CConfiguration configuration) {
    this.port = configuration.getInt(Constants.CFG_SERVER_PORT, 7777);

    server = new Server(port);
    Context context = new Context(server, "/", Context.SESSIONS);
    context.addEventListener(new MockGuiceContextListener(configuration));
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

    private final String connectionString;
    private ServletContext servletContext;

    public MockGuiceContextListener(CConfiguration configuration) {
      this.connectionString = configuration.get(Constants.CFG_JDBC_CONNECTION_STRING);
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
        public void configureServlets() {
          MapBinder<String, String> configBinder = MapBinder.newMapBinder(binder(), String.class, String.class,
            Names.named("passport.config"));

          //Bind ResT Resources
          bind(AccountHandler.class);
          //TODO: Bind other ReST resources

          bind(DataManagementService.class).to(DataManagementServiceImpl.class);
          bind(AuthenticatorService.class).to(AuthenticatorServiceImpl.class);
          bind(AccountDAO.class).to(AccountDBAccess.class);
          bind(VpcDAO.class).to(VpcDBAccess.class);
          bind(NonceDAO.class).to(NonceDBAccess.class);
          bind(ProfanityFilter.class).to(ProfanityFilterFileAccess.class);


          bind(GuiceContainer.class).asEagerSingleton();
          bind(DefaultServlet.class).asEagerSingleton();
          serve("/*").with(DefaultServlet.class);
          filter("/passport/*").through(GuiceContainer.class);
        }

        @Provides
        ConnectionPoolDataSource provider() {
          JDBCPooledDataSource jdbcDataSource = new JDBCPooledDataSource();
          jdbcDataSource.setUrl(connectionString);
          return jdbcDataSource;
        }
      });
    }
  }
}






