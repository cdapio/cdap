/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.security.server;

import co.cask.cdap.common.ServiceBindException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Configuration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.conf.SConfiguration;
import co.cask.cdap.common.discovery.ResolvingDiscoverable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.commons.lang.StringUtils;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jboss.resteasy.plugins.server.servlet.HttpServletDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Jetty service for External Authentication.
 */
public class ExternalAuthenticationServer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ExternalAuthenticationServer.class);
  public static final String NAMED_EXTERNAL_AUTH = "external.auth";

  private final int port;
  private final int maxThreads;
  private final Map<String, Object> handlers;
  private final DiscoveryService discoveryService;
  private final CConfiguration cConfiguration;
  private final SConfiguration sConfiguration;
  private final AuditLogHandler auditLogHandler;
  private final GrantAccessToken grantAccessToken;
  private final AbstractAuthenticationHandler authenticationHandler;

  private Cancellable serviceCancellable;
  private Server server;
  private InetAddress bindAddress;

  /**
   * Constants for a valid JSON response.
   */
  public static final class ResponseFields {
    public static final String TOKEN_TYPE = "token_type";
    public static final String TOKEN_TYPE_BODY = "Bearer";
    public static final String ACCESS_TOKEN = "access_token";
    public static final String EXPIRES_IN = "expires_in";
  }

  /**
   * Constants for Handler types.
   */
  public static final class HandlerType {
    public static final String AUTHENTICATION_HANDLER = "AuthenticationHandler";
    public static final String GRANT_TOKEN_HANDLER = "GrantTokenHandler";
  }

  @Inject
  public ExternalAuthenticationServer(CConfiguration cConfiguration, SConfiguration sConfiguration,
                                      DiscoveryService discoveryService,
                                      @Named("security.handlers.map") Map<String, Object> handlers,
                                      @Named(NAMED_EXTERNAL_AUTH) AuditLogHandler auditLogHandler) {
    this.port = cConfiguration.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED) ?
      cConfiguration.getInt(Constants.Security.AuthenticationServer.SSL_PORT) :
      cConfiguration.getInt(Constants.Security.AUTH_SERVER_BIND_PORT);
    this.maxThreads = cConfiguration.getInt(Constants.Security.MAX_THREADS);
    this.handlers = handlers;
    this.discoveryService = discoveryService;
    this.cConfiguration = cConfiguration;
    this.sConfiguration = sConfiguration;
    this.grantAccessToken = (GrantAccessToken) handlers.get(HandlerType.GRANT_TOKEN_HANDLER);
    this.authenticationHandler = (AbstractAuthenticationHandler) handlers.get(HandlerType.AUTHENTICATION_HANDLER);
    this.auditLogHandler = auditLogHandler;
  }

  /**
   * Get the InetSocketAddress of the server.
   *
   * @return InetSocketAddress of server.
   */
  public InetSocketAddress getSocketAddress() {
    if (!server.isRunning()) {
      throw new IllegalStateException("Server not started yet");
    }

    // assumes we only have one connector
    Connector connector = server.getConnectors()[0];
    return new InetSocketAddress(connector.getHost(), connector.getLocalPort());
  }

  @Override
  protected void startUp() throws Exception {
    try {
      server = new Server();

      try {
        bindAddress = InetAddress.getByName(cConfiguration.get(Constants.Security.AUTH_SERVER_BIND_ADDRESS));
      } catch (UnknownHostException e) {
        LOG.error("Error finding host to connect to.", e);
        throw e;
      }

      QueuedThreadPool threadPool = new QueuedThreadPool();
      threadPool.setMaxThreads(maxThreads);
      server.setThreadPool(threadPool);

      initHandlers();

      ServletContextHandler context = new ServletContextHandler();
      context.setServer(server);
      context.addServlet(HttpServletDispatcher.class, "/");
      context.addEventListener(new AuthenticationGuiceServletContextListener(handlers));
      context.setSecurityHandler(authenticationHandler);

      // Status endpoint should be handled without the authentication
      ContextHandler statusContext = new ContextHandler();
      statusContext.setContextPath(Constants.EndPoints.STATUS);
      statusContext.setServer(server);
      statusContext.setHandler(new StatusRequestHandler());

      if (cConfiguration.getBoolean(Constants.Security.SSL.EXTERNAL_ENABLED, false)) {
        SslContextFactory sslContextFactory = new SslContextFactory();
        String keyStorePath = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PATH);
        String keyStorePassword = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_PASSWORD);
        String keyStoreType = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYSTORE_TYPE,
                                                 Constants.Security.AuthenticationServer.DEFAULT_SSL_KEYSTORE_TYPE);
        String keyPassword = sConfiguration.get(Constants.Security.AuthenticationServer.SSL_KEYPASSWORD);

        Preconditions.checkArgument(keyStorePath != null, "Key Store Path Not Configured");
        Preconditions.checkArgument(keyStorePassword != null, "KeyStore Password Not Configured");

        sslContextFactory.setKeyStorePath(keyStorePath);
        sslContextFactory.setKeyStorePassword(keyStorePassword);
        sslContextFactory.setKeyStoreType(keyStoreType);
        if (keyPassword != null && keyPassword.length() != 0) {
          sslContextFactory.setKeyManagerPassword(keyPassword);
        }

        String trustStorePath = cConfiguration.get(Constants.Security.AuthenticationServer.SSL_TRUSTSTORE_PATH);
        if (StringUtils.isNotEmpty(trustStorePath)) {
          String trustStorePassword =
            cConfiguration.get(Constants.Security.AuthenticationServer.SSL_TRUSTSTORE_PASSWORD);
          String trustStoreType = cConfiguration.get(Constants.Security.AuthenticationServer.SSL_TRUSTSTORE_TYPE,
                                                     Constants.Security.AuthenticationServer.DEFAULT_SSL_KEYSTORE_TYPE);

          // SSL handshaking will involve requesting for a client certificate, if cert is not provided
          // server continues with the connection but the client is considered to be unauthenticated
          sslContextFactory.setWantClientAuth(true);

          sslContextFactory.setTrustStore(trustStorePath);
          sslContextFactory.setTrustStorePassword(trustStorePassword);
          sslContextFactory.setTrustStoreType(trustStoreType);
          sslContextFactory.setValidateCerts(true);
        }
        // TODO Figure out how to pick a certificate from key store

        SslSelectChannelConnector sslConnector = new SslSelectChannelConnector(sslContextFactory);
        sslConnector.setHost(bindAddress.getCanonicalHostName());
        sslConnector.setPort(port);
        server.setConnectors(new Connector[]{sslConnector});
      } else {
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setHost(bindAddress.getCanonicalHostName());
        connector.setPort(port);
        server.setConnectors(new Connector[]{connector});
      }

      HandlerCollection handlers = new HandlerCollection();
      handlers.addHandler(statusContext);
      handlers.addHandler(context);
      // AuditLogHandler must be last, since it needs the response that was sent to the client
      handlers.addHandler(auditLogHandler);

      server.setHandler(handlers);
    } catch (Exception e) {
      LOG.error("Error while starting Authentication Server.", e);
    }

    try {
      server.start();
    } catch (Exception e) {
      if ((Throwables.getRootCause(e) instanceof BindException)) {
        throw new ServiceBindException("Authentication Server", bindAddress.getCanonicalHostName(), port, e);
      }
      throw e;
    }

    // assumes we only have one connector
    Connector connector = server.getConnectors()[0];
    InetSocketAddress inetSocketAddress = new InetSocketAddress(connector.getHost(), connector.getLocalPort());
    serviceCancellable = discoveryService.register(
      ResolvingDiscoverable.of(new Discoverable(Constants.Service.EXTERNAL_AUTHENTICATION, inetSocketAddress)));
  }

  /**
   * Initializes the handlers.
   */
  private void initHandlers() throws Exception {
    Map<String, String> handlerProps = new HashMap<>();

    // used by CertificateAuthenticationHandler (see CDAP-7287)
    copyPropIfExists(handlerProps, cConfiguration, "security.auth.server.ssl.truststore.path");
    copyPropIfExists(handlerProps, cConfiguration, "security.auth.server.ssl.truststore.type");
    copyPropIfExists(handlerProps, cConfiguration, "security.auth.server.ssl.truststore.password");
    // used by AbstractAuthenticationHandler
    copyPropIfExists(handlerProps, cConfiguration, Constants.Security.SSL.EXTERNAL_ENABLED);
    // used by BasicAuthenticationHandler
    copyPropIfExists(handlerProps, cConfiguration, Constants.Security.BASIC_REALM_FILE);
    // used by BJASPIAuthenticationHandler
    copyPropIfExists(handlerProps, cConfiguration, Constants.Security.LOGIN_MODULE_CLASS_NAME);

    copyProps(handlerProps, getAuthHandlerConfigs(cConfiguration));
    copyProps(handlerProps, getAuthHandlerConfigs(sConfiguration));

    authenticationHandler.init(handlerProps);
    grantAccessToken.init();
  }

  // we don't leverage Map#putAll, because we want to warn if overwriting some keys
  private void copyProps(Map<String, String> toProps, Map<String, String> fromProps) {
    for (Map.Entry<String, String> entry : fromProps.entrySet()) {
      String previousVal = toProps.put(entry.getKey(), entry.getValue());
      if (previousVal != null && !previousVal.equals(entry.getValue())) {
        // this can unlikely, but can happen if one of the non-prefixed parameters that we expose to authentication
        // handlers is also prefixed with the Constants.Security.AUTH_HANDLER_CONFIG_BASE
        LOG.warn("Overriding property '{}'", entry.getKey());
      }
    }
  }

  private void copyPropIfExists(Map<String, String> toProps, Configuration fromConf, String property) {
    String value = fromConf.get(property);
    if (value != null) {
      toProps.put(property, value);
    }
  }

  private Map<String, String> getAuthHandlerConfigs(Configuration configuration) {
    String prefix = Constants.Security.AUTH_HANDLER_CONFIG_BASE;
    int prefixLen = prefix.length();
    // since its a regex match, we want to look for the character '.', and not match any character
    String configRegex = "^" + prefix.replace(".", "\\.");

    Map<String, String> props = new HashMap<>();
    for (Map.Entry<String, String> pair : configuration.getValByRegex(configRegex).entrySet()) {
      String key = pair.getKey();
      // we get the value via the conf, because conf#getValByRegex does not do variable substitution
      String value = configuration.get(key);
      props.put(key.substring(prefixLen), value);
    }
    return props;
  }

  @Override
  protected Executor executor(State state) {
    final AtomicInteger id = new AtomicInteger();
    //noinspection NullableProblems
    final Thread.UncaughtExceptionHandler h = new Thread.UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
      }
    };
    return new Executor() {
      @Override
      public void execute(Runnable runnable) {
        Thread t = new Thread(runnable, String.format("ExternalAuthenticationServer-%d", id.incrementAndGet()));
        t.setUncaughtExceptionHandler(h);
        t.start();
      }
    };
  }

  @Override
  protected void shutDown() {
    try {
      serviceCancellable.cancel();
      server.stop();
      grantAccessToken.destroy();
    } catch (Exception e) {
      LOG.error("Error stopping Authentication Server.", e);
    }
  }
}
