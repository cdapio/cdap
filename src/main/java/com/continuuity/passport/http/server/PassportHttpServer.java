package com.continuuity.passport.http.server;


import com.continuuity.common.conf.CConfiguration;
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

  private final int gracefulShutdownTime;
  private final int port;

  private final Map<String, String> configuration;

  public PassportHttpServer(int port, Map<String, String> configuration, int gracefulShutdownTime) {
    this.port = port;
    this.configuration = configuration;
    this.gracefulShutdownTime = gracefulShutdownTime;
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

    CConfiguration conf = CConfiguration.create();
    conf.addResource("continuuity-passport.xml");

    String jdbcType = conf.get("passport.jdbc.type");
    String connectionString  = conf.get("passport.jdbc.connection.string");
    System.out.println(jdbcType);
    int port = Integer.parseInt(conf.get("passport.http.server.port"));
    int gracefulShutdownTime = Integer.parseInt(conf.get("passport.http.graceful.shutdown.time"));

    config.put("jdbcType", jdbcType);
    config.put("connectionString",connectionString);

    Realm realm = new JDBCAuthrozingRealm(config);

    org.apache.shiro.mgt.SecurityManager securityManager = new DefaultSecurityManager(realm);
    SecurityUtils.setSecurityManager(securityManager);

    PassportHttpServer server = new PassportHttpServer(port, config, gracefulShutdownTime);
    server.start();


  }
}
