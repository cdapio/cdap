package com.continuuity.passport.testhelper;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.passport.Constants;
import com.continuuity.passport.http.modules.ShiroGuiceModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.SecurityManager;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

/**
 * Mock Server to test out the Http endpoints.
 * Uses Mock implementations.
 */
public class TestPassportServer {

  private final Server server;
  private final int port;


  public TestPassportServer(CConfiguration configuration) {
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

  /**
   *
   */
  public class MockGuiceContextListener extends GuiceServletContextListener {

    private final String connectionString;
    private final String profaneWordsPath;
    private ServletContext servletContext;
    private final CConfiguration configuration;

    public MockGuiceContextListener(CConfiguration configuration) {
      this.connectionString = configuration.get(Constants.CFG_JDBC_CONNECTION_STRING);
      this.profaneWordsPath = configuration.get(Constants.CFG_PROFANE_WORDS_FILE_PATH);
      this.configuration = configuration;
    }

    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
      this.servletContext = servletContextEvent.getServletContext();
      super.contextInitialized(servletContextEvent);
    }

    @Override
    protected Injector getInjector() {
      Injector injector = Guice.createInjector(new MockGuiceBindings(configuration), new ShiroGuiceModule());
      org.apache.shiro.mgt.SecurityManager securityManager = injector.getInstance(SecurityManager.class);
      SecurityUtils.setSecurityManager(securityManager);

      return injector;
  }
 }
}






