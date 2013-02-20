package com.continuuity.passport.http.modules;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import java.util.Map;


/**
 *  Guice module to glue in Jetty server and bindings
 */
public class PassportGuiceServletContextListener extends GuiceServletContextListener {


  private final Map<String, String> config;
  private ServletContext servletContext;

  public PassportGuiceServletContextListener(Map<String, String> config) {
    this.config = config;
  }

  @Override
  public void contextInitialized(ServletContextEvent servletContextEvent) {
    this.servletContext = servletContextEvent.getServletContext();
    super.contextInitialized(servletContextEvent);
  }

  /**
   * Override this method to create (or otherwise obtain a reference to) your
   * injector.
   */
  @Override
  protected Injector getInjector() {
    return Guice.createInjector(new PassportGuiceBindings(config));
    //TODO: Add shiro module
  }
}
