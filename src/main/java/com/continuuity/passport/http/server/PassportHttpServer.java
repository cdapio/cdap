package com.continuuity.passport.http.server;


import com.continuuity.passport.dal.db.JDBCAuthrozingRealm;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.realm.Realm;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.management.MBeanContainer;




import javax.management.MBeanServer;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

public class PassportHttpServer  {

  private int gracefulShutdownTime = 1000;
  private int port = 7777;
  private void start() {
    try{
      Server server = new Server(port);
      server.setStopAtShutdown(true);
      server.setGracefulShutdown(gracefulShutdownTime);

      Context context = new Context(server, "/", Context.SESSIONS);

      context.addServlet(new ServletHolder(new ServletContainer(
        new PackagesResourceConfig("com.continuuity.passport.http"))), "/*");

    //  context.addFilter(ContinuuitySecurityFilter.class,"/passport/v1/*",0);

        //JMX jetty
      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
      MBeanContainer mBeanContainer = new MBeanContainer(mBeanServer);
      server.getContainer().addEventListener(mBeanContainer);
      mBeanContainer.start();

      server.start();
      server.join();

    }
    catch(Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String [] args) {

    Map<String,String> config = new HashMap<String,String>();

    //TODO: Move this configurations to a central place
    config.put("jdbcType","mysql");
    config.put("connectionString","jdbc:mysql://a101.dev.sl:3306/continuuity?user=passport_user");
    //config.put("connectionString","jdbc:mysql://localhost:3306/continuuity?user=passport_user");

    Realm realm = new JDBCAuthrozingRealm(config);

    org.apache.shiro.mgt.SecurityManager securityManager = new DefaultSecurityManager(realm);
    SecurityUtils.setSecurityManager(securityManager);

    PassportHttpServer server = new PassportHttpServer();
    server.start();


  }
}
