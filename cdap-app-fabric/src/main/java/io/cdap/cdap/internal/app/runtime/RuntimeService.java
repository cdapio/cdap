/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.api.messaging.MessagingContext;
import io.cdap.cdap.common.HttpExceptionHandler;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.cdap.internal.app.runtime.monitor.RemoteExecutionLogProcessor;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.messaging.context.MultiThreadMessagingContext;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Service to manage metadata in CDAP. This service serves the HTTP endpoints defined in {@link RuntimeHttpHandler}.
 */
public class RuntimeService extends AbstractIdleService {
    private static final Logger LOG = LoggerFactory.getLogger(RuntimeService.class);
    private final DiscoveryService discoveryService;
    private final NettyHttpService httpService;
    private Cancellable cancelDiscovery;
    private final MessagingContext messagingContext;

    @Inject
    RuntimeService(CConfiguration cConf, SConfiguration sConf,
                   MessagingService messagingService,
//                   RemoteExecutionLogProcessor logProcessor,
                   DiscoveryService discoveryService,
                   @Named(Constants.Runtime.HANDLERS_NAME) Set<HttpHandler> handlers) {
        this.discoveryService = discoveryService;
        this.messagingContext = new MultiThreadMessagingContext(messagingService);

        NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.RUNTIME)
                .setHttpHandlers(handlers)
                .setExceptionHandler(new HttpExceptionHandler())
                .setHost(cConf.get(Constants.Runtime.SERVICE_BIND_ADDRESS))
                .setPort(cConf.getInt(Constants.Runtime.SERVICE_BIND_PORT))
                .setWorkerThreadPoolSize(cConf.getInt(Constants.Runtime.SERVICE_WORKER_THREADS))
                .setExecThreadPoolSize(cConf.getInt(Constants.Runtime.SERVICE_EXEC_THREADS));

        if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
            new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
        }
        httpService = builder.build();
    }

    @Override
    protected void startUp() throws Exception {
        LOG.info("Starting Runtime Service");
        httpService.start();
        InetSocketAddress socketAddress = httpService.getBindAddress();
        LOG.info("Runtime service running at {}", socketAddress);
        cancelDiscovery = discoveryService.register(
                ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.RUNTIME, httpService)));

    }

    @Override
    protected void shutDown() throws Exception {
        LOG.debug("Shutting down Runtime Service");
        cancelDiscovery.cancel();
        httpService.stop();
        LOG.info("Runtime HTTP service stopped");
    }
}
