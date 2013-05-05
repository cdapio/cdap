/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.modules;

import com.continuuity.common.conf.CConfiguration;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.SecurityManager;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;


/**
 *  Guice module to glue in Jetty server and bindings.
 */
public class PassportGuiceServletContextListener extends GuiceServletContextListener {

  private final CConfiguration configuration;
  private ServletContext servletContext;

  public PassportGuiceServletContextListener(CConfiguration configuration) {
    this.configuration = configuration;
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
    Injector injector =  Guice.createInjector(new PassportGuiceBindings(configuration),
                                              new ShiroGuiceModule());
    SecurityManager securityManager = injector.getInstance(SecurityManager.class);
    SecurityUtils.setSecurityManager(securityManager);
    return injector;
  }
}
