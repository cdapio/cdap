/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.security.tools;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.runtime.DaemonMain;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.guice.CoreSecurityRuntimeModule;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Base64;
import java.util.Collections;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Http server that serves access tokens.
 */
public class AccessTokenGeneratorService extends DaemonMain {

  private static final Logger LOG = LoggerFactory.getLogger(AccessTokenGeneratorService.class);
  // Http handler configuration
  static final String ADDRESS_CONFIG = "access.token.generator.bind.address";
  static final String ADDRESS_DEFAULT = "127.0.0.1";
  static final String PORT_CONFIG = "access.token.generator.bind.port";
  static final int PORT_DEFAULT = 11030;
  // HttpHandler has only one caller, and is accessed infrequently so one worker thread is sufficient
  static final String WORKER_POOL_SIZE_CONFIG = "access.token.generator.worker.pool.size";
  static final int WORKER_POOL_SIZE_DEFAULT = 1;
  static final String EXEC_POOL_SIZE_CONFIG = "access.token.generator.executor.pool.size";
  static final int EXEC_POOL_SIZE_DEFAULT = 0;

  private NettyHttpService httpService;
  private TokenGeneratorHttpHandler handler;

  @VisibleForTesting
  InetSocketAddress getBindAddress() {
    return httpService.getBindAddress();
  }

  @VisibleForTesting
  TokenManager getTokenManager() {
    return handler.tokenManager;
  }

  @VisibleForTesting
  Codec<AccessToken> getTokenCodec() {
    return handler.tokenCodec;
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting AccessTokenGeneratorService.");
    handler.tokenManager.startUp();
    httpService.start();
  }

  @Override
  public void stop() {
    LOG.info("Stopping AccessTokenGeneratorService.");
    try {
      httpService.stop();
    } catch (Exception e) {
      LOG.warn("Exception when stopping AccessTokenGeneratorService", e);
    }
    handler.tokenManager.stopAndWait();
  }

  @Override
  public void destroy() {
  }

  @Path(Constants.Gateway.INTERNAL_API_VERSION_3 + "/accesstoken")
  public static class TokenGeneratorHttpHandler extends AbstractHttpHandler {

    private final CConfiguration cConf;
    private final TokenManager tokenManager;
    private final Codec<AccessToken> tokenCodec;

    @Inject
    TokenGeneratorHttpHandler(CConfiguration cConf, TokenManager tokenManager,
        Codec<AccessToken> tokenCodec) {
      this.cConf = cConf;
      this.tokenManager = tokenManager;
      this.tokenCodec = tokenCodec;
    }

    @GET
    @Path("/")
    public void getToken(FullHttpRequest request, HttpResponder responder,
        @QueryParam("username") String username,
        @QueryParam("identifierType") @DefaultValue("INTERNAL") String identifierType)
        throws IOException, BadRequestException {
      UserIdentity.IdentifierType type;
      try {
        type = UserIdentity.IdentifierType.valueOf(identifierType);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(String.format("Invalid identifierType. Valid values: %s",
            Joiner.on(',').join(UserIdentity.IdentifierType.values())));
      }
      long expirationTime = cConf.getLong(Constants.Security.TOKEN_EXPIRATION);
      long issueTime = System.currentTimeMillis();
      long expireTime = issueTime + expirationTime;
      UserIdentity userIdentity = new UserIdentity(username, type,
          Collections.emptyList(),
          issueTime, expireTime);
      AccessToken token = tokenManager.signIdentifier(userIdentity);
      String encodedToken = Base64.getEncoder().encodeToString(tokenCodec.encode(token));
      responder.sendString(HttpResponseStatus.OK, encodedToken);
    }
  }

  private Injector createInjector(CConfiguration cConf) {
    return Guice.createInjector(
        new ConfigModule(cConf),
        new IOModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            // We don't do anything with metrics, so just bind to the no-op implementation
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          }
        },
        CoreSecurityRuntimeModule.getDistributedModule(cConf));
  }

  @Override
  public void init(String[] args) throws Exception {
    Option portOption = new Option("p", "port", true, "Server port.");
    portOption.setType(Integer.class);
    Option workerPoolSizeOption = new Option("w", "workerPoolSize", true,
        "Size of worker thread pool.");
    workerPoolSizeOption.setType(Integer.class);
    Option execPoolSizeOption = new Option("e", "execPoolSize", true,
        "Size of executor thread pool.");
    execPoolSizeOption.setType(Integer.class);
    Option tokenExpirationOption = new Option("t", "tokenExpiration", true,
        "Access token expiration time in milliseconds.");
    tokenExpirationOption.setType(Integer.class);

    Options options = new Options()
        .addOption(new Option("h", "help", false, "Print this usage message."))
        .addOption(new Option("a", "address", true, "Server IP address."))
        .addOption(portOption)
        .addOption(workerPoolSizeOption)
        .addOption(execPoolSizeOption)
        .addOption(tokenExpirationOption);

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    CConfiguration cConf = CConfiguration.create();
    if (commandLine.hasOption("a")) {
      cConf.set(ADDRESS_CONFIG, commandLine.getOptionValue("a"));
    }
    if (commandLine.hasOption("p")) {
      cConf.setInt(PORT_CONFIG, (int) commandLine.getParsedOptionValue("p"));
    }
    if (commandLine.hasOption("w")) {
      cConf.setInt(WORKER_POOL_SIZE_CONFIG, (int) commandLine.getParsedOptionValue("w"));
    }
    if (commandLine.hasOption("e")) {
      cConf.setInt(EXEC_POOL_SIZE_CONFIG, (int) commandLine.getParsedOptionValue("e"));
    }
    if (commandLine.hasOption("t")) {
      cConf.setInt(Constants.Security.TOKEN_EXPIRATION,
          (int) commandLine.getParsedOptionValue("e"));
    }

    Injector injector = createInjector(cConf);

    this.handler = injector.getInstance(TokenGeneratorHttpHandler.class);
    SConfiguration sConf = injector.getInstance(SConfiguration.class);
    NettyHttpService.Builder builder = NettyHttpService.builder("accesstoken.generator")
        .setHost(cConf.get(ADDRESS_CONFIG, ADDRESS_DEFAULT))
        .setPort(cConf.getInt(PORT_CONFIG, PORT_DEFAULT))
        .setExecThreadPoolSize(cConf.getInt(EXEC_POOL_SIZE_CONFIG, EXEC_POOL_SIZE_DEFAULT))
        .setWorkerThreadPoolSize(cConf.getInt(WORKER_POOL_SIZE_CONFIG, WORKER_POOL_SIZE_DEFAULT))
        .setExceptionHandler(new HttpExceptionHandler())
        .setHttpHandlers(handler);
    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    httpService = builder.build();
  }

  public static void main(String[] args) throws Exception {
    new AccessTokenGeneratorService().doMain(args);
  }
}
