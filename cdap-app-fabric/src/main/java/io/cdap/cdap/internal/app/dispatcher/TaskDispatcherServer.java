package io.cdap.cdap.internal.app.dispatcher;


import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.SConfiguration;
import io.cdap.cdap.common.discovery.ResolvingDiscoverable;
import io.cdap.cdap.common.discovery.URIScheme;
import io.cdap.cdap.common.http.CommonNettyHttpServiceBuilder;
import io.cdap.cdap.common.security.HttpsEnabler;
import io.cdap.http.HttpHandler;
import io.cdap.http.NettyHttpService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Set;


public class TaskDispatcherServer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(TaskDispatcherServer.class);

  private final CConfiguration cConf;
  private final SConfiguration sConf;

  private final DiscoveryService discoveryService;
  private final NettyHttpService httpService;
  private Cancellable cancelDiscovery;
  private InetSocketAddress bindAddress;


  @Inject
  public TaskDispatcherServer(CConfiguration cConf, SConfiguration sConf,
                              DiscoveryService discoveryService,
                              @Named(Constants.TaskDispatcher.HANDLER_NAME) Set<HttpHandler> handlers) {
    this.discoveryService = discoveryService;
    this.cConf = cConf;
    this.sConf = sConf;

    NettyHttpService.Builder builder = new CommonNettyHttpServiceBuilder(cConf, Constants.Service.TASK_DISPATCHER)
      .setHost(cConf.get(Constants.TaskDispatcher.ADDRESS))
      .setPort(cConf.getInt(Constants.TaskDispatcher.PORT))
      .setExecThreadPoolSize(cConf.getInt(Constants.TaskDispatcher.EXEC_THREADS))
      .setBossThreadPoolSize(cConf.getInt(Constants.TaskDispatcher.BOSS_THREADS))
      .setWorkerThreadPoolSize(cConf.getInt(Constants.TaskDispatcher.WORKER_THREADS))
      .setHttpHandlers(handlers);

    if (cConf.getBoolean(Constants.Security.SSL.INTERNAL_ENABLED)) {
      new HttpsEnabler().configureKeyStore(cConf, sConf).enable(builder);
    }
    this.httpService = builder.build();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting TaskDispatcherServer");

    httpService.start();
    bindAddress = httpService.getBindAddress();
    cancelDiscovery = discoveryService.register(
      ResolvingDiscoverable.of(URIScheme.createDiscoverable(Constants.Service.TASK_DISPATCHER, httpService)));

    LOG.info("TaskDispatcher HTTP server started on {}", httpService.getBindAddress());
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down TaskDispatcherServer");
    try {
      cancelDiscovery.cancel();
    } finally {
      httpService.stop();
    }
    LOG.info("TaskDispatcher HTTP server stopped");
  }

  public InetSocketAddress getBindAddress() {
    return bindAddress;
  }
}
