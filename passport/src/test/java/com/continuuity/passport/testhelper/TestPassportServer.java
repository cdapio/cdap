package com.continuuity.passport.testhelper;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.passport.Constants;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.service.SecurityService;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.OrganizationDAO;
import com.continuuity.passport.dal.ProfanityFilter;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.dal.db.AccountDBAccess;
import com.continuuity.passport.dal.db.NonceDBAccess;
import com.continuuity.passport.dal.db.OrganizationDBAccess;
import com.continuuity.passport.dal.db.ProfanityFilterFileAccess;
import com.continuuity.passport.dal.db.VpcDBAccess;
import com.continuuity.passport.http.handlers.AccountHandler;
import com.continuuity.passport.http.handlers.ActivationNonceHandler;
import com.continuuity.passport.http.handlers.OrganizationHandler;
import com.continuuity.passport.http.handlers.SessionNonceHandler;
import com.continuuity.passport.http.handlers.VPCHandler;
import com.continuuity.passport.http.modules.ShiroGuiceModule;
import com.continuuity.passport.impl.AuthenticatorServiceImpl;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.continuuity.passport.impl.SecuritySeviceImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.gson.JsonObject;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.SecurityManager;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Mock Server to test out the Http endpoints.
 * Uses Mock implementations.
 */
public class TestPassportServer {

  private final NettyHttpService server;
  private final int port;
  private final String jdbcType;
  private final String connectionString;
  private final String profaneWordsPath;


  public TestPassportServer(CConfiguration configuration) {

    this.port = configuration.getInt(Constants.CFG_SERVER_PORT, 7777);
    jdbcType = configuration.get(Constants.CFG_JDBC_TYPE, Constants.DEFAULT_JDBC_TYPE);
    connectionString = configuration.get(Constants.CFG_JDBC_CONNECTION_STRING,
                                         Constants.DEFAULT_JDBC_CONNECTION_STRING);
    profaneWordsPath = configuration.get(Constants.CFG_PROFANE_WORDS_FILE_PATH,
                                         Constants.DEFAULT_PROFANE_WORDS_FILE_PATH);

    Injector injector = Guice.createInjector(new MockGuiceModule(configuration), new ShiroGuiceModule());
    SecurityManager securityManager = injector.getInstance(SecurityManager.class);
    SecurityUtils.setSecurityManager(securityManager);

    List<HttpHandler> handlers = Lists.newArrayList();
    handlers.add(injector.getInstance(AccountHandler.class));
    handlers.add(injector.getInstance(VPCHandler.class));
    handlers.add(injector.getInstance(OrganizationHandler.class));
    handlers.add(injector.getInstance(ActivationNonceHandler.class));
    handlers.add(injector.getInstance(SessionNonceHandler.class));


    server = NettyHttpService.builder()
                                .setPort(port)
                                .addHttpHandlers(handlers).build();

  }

  public void start() throws Exception {
    server.startAndWait();
  }

  public boolean isStarted() {
    return server.isRunning();
  }

  public void stop() throws Exception {
    server.stop();
  }


  public static String request(HttpUriRequest uri) throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpResponse response = client.execute(uri);
    System.out.println(response.toString());
    assertTrue(response.getStatusLine().getStatusCode() == 200);
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ByteStreams.copy(response.getEntity().getContent(), bos);
    String result = bos.toString("UTF-8");
    bos.close();
    return result;
  }

  public static String getCompany(String id, String name){
    JsonObject object = new JsonObject();
    object.addProperty("id", id);
    object.addProperty("name", name);
    return object.toString();
  }
}






