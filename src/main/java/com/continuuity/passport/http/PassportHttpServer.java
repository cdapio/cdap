package com.continuuity.passport.http;


import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

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

      server.start();
      server.join();

    }
    catch(Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String [] args) {
    PassportHttpServer server = new PassportHttpServer();
    server.start();
  }
}
