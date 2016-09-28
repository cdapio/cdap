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

package co.cask.cdap.logging.gateway.handlers;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.RootLocationFactory;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.logging.LoggingConfiguration;
import co.cask.cdap.logging.save.LogSaverTableUtil;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.tephra.TransactionExecutorFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * v3 {@link HttpHandler} to handle deletion in case of inconsistencies in log files and metadata
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3)
@Beta
public class LogAdminHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(LogAdminHandler.class);

  private final RootLocationFactory rootLocationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final String logBaseDir;
  private final LogSaverTableUtil tableUtil;
  private final TransactionExecutorFactory transactionExecutorFactory;
  private final Impersonator impersonator;
  private final AuthorizationEnforcer authorizationEnforcer;

  @Inject
  public LogAdminHandler(AuthorizationEnforcer authorizationEnforcer, final LogSaverTableUtil tableUtil,
                         TransactionExecutorFactory txExecutorFactory,
                         RootLocationFactory rootLocationFactory,
                         NamespacedLocationFactory namespacedLocationFactory, CConfiguration cConf,
                         Impersonator impersonator) {
    this.authorizationEnforcer = authorizationEnforcer;
    this.rootLocationFactory = rootLocationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.logBaseDir =  cConf.get(LoggingConfiguration.LOG_BASE_DIR);
    this.tableUtil = tableUtil;
    this.transactionExecutorFactory = txExecutorFactory;
    this.impersonator = impersonator;

  }

  @GET
  @Path("namespaces/{namespace-id}/logs/inspect")
  public void inspect(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId) {
    getInconsistencies(namespaceId);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  private void getInconsistencies(String namespaceId) {
    LOG.info("Got in logs inspect");
  }

  @POST
  @Path("namespaces/{namespace-id}/logs/repair")
  public void repair(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespace) {
    LOG.info("Got in logs repair");
    responder.sendStatus(HttpResponseStatus.OK);
  }
}
