package com.continuuity.passport.http.server;


import com.continuuity.passport.dal.db.JDBCAuthrozingRealm;
import com.continuuity.passport.http.modules.PassportGuiceServletContextListener;
import com.google.inject.servlet.GuiceFilter;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.management.MBeanContainer;

import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

public class PassportHttpServer {

  private final int gracefulShutdownTime = 1000;
  private final int port;

  private final Map<String, String> configuration;

  public PassportHttpServer(int port, Map<String, String> configuration) {
    this.port = port;
    this.configuration = configuration;
  }

  private void start() {

    try {

      Server server = new Server(port);
      server.setStopAtShutdown(true);
      server.setGracefulShutdown(gracefulShutdownTime);


      Context context = new Context(server, "/", Context.SESSIONS);
      context.addEventListener(new PassportGuiceServletContextListener(configuration));
      context.addServlet(DefaultServlet.class, "/");
      context.addFilter(GuiceFilter.class, "/*", 0);

      //JMX jetty
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
      server.getContainer().addEventListener(mBeanContainer);
      mBeanContainer.start();

      server.start();
      server.join();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {

    Map<String, String> config = new HashMap<String, String>();

    //TODO: READ Config from the file.
    config.put("jdbcType", "mysql");
    //config.put("connectionString","jdbc:mysql://ppdb101.joyent.continuuity.net:3306/continuuity?user=passport_user");
    config.put("connectionString", "jdbc:mysql://localhost:3306/continuuity?user=passport_user");

    Realm realm = new JDBCAuthrozingRealm(config);

    org.apache.shiro.mgt.SecurityManager securityManager = new DefaultSecurityManager(realm);
    SecurityUtils.setSecurityManager(securityManager);

    int port = 7777; //TODO: Read from Config file
    PassportHttpServer server = new PassportHttpServer(port, config);
    server.start();


  }
}
