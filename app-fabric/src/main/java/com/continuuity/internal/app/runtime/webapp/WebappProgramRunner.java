package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.common.utils.Networks;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.common.Cancellable;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryService;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

/**
 * Run Webapp server.
 */
public class WebappProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(WebappProgramRunner.class);

  private final ServiceAnnouncer serviceAnnouncer;
  private final DiscoveryService discoveryService;
  private final InetAddress hostname;
  private final WebappHttpHandlerFactory handlerFactory;

  @Inject
  public WebappProgramRunner(ServiceAnnouncer serviceAnnouncer, DiscoveryService discoveryService,
                             @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                             WebappHttpHandlerFactory handlerFactory) {
    this.serviceAnnouncer = serviceAnnouncer;
    this.discoveryService = discoveryService;
    this.hostname = hostname;
    this.handlerFactory = handlerFactory;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {

      Type processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.WEBAPP, "Only WEBAPP process type is supported.");

      LOG.info("Initializing web app for app {} with jar {}", program.getApplicationId(),
               program.getJarLocation().getName());

      String serviceName = getServiceName(Type.WEBAPP, program);
      Preconditions.checkNotNull(serviceName, "Cannot determine service name for program %s", program.getName());
      LOG.info("Got service name {}", serviceName);

      // Start netty server
      // TODO: add metrics reporting
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableList.of(handlerFactory.createHandler(program.getJarLocation())));
      builder.setHost(hostname.getCanonicalHostName());
      NettyHttpService httpService = builder.build();
      httpService.startAndWait();
      final InetSocketAddress address = httpService.getBindAddress();

      RunId runId = RunIds.generate();

      // Register service, and the serving host names.
      final List<Cancellable> cancellables = Lists.newArrayList();
      LOG.info("Webapp {} running on address {} registering as {}", program.getApplicationId(), address, serviceName);
      cancellables.add(serviceAnnouncer.announce(serviceName, address.getPort()));

      for (String hname : getServingHostNames(program.getJarLocation().getInputStream())) {
        final String sname = Type.WEBAPP.name().toLowerCase() + "/" + hname;

        LOG.info("Webapp {} running on address {} registering as {}", program.getApplicationId(), address, sname);
        cancellables.add(discoveryService.register(new Discoverable() {
          @Override
          public String getName() {
            return sname;
          }

          @Override
          public InetSocketAddress getSocketAddress() {
            return address;
          }
        }));
      }

      return new WebappProgramController(program.getName(), runId, httpService, new Cancellable() {
        @Override
        public void cancel() {
          for (Cancellable cancellable : cancellables) {
            cancellable.cancel();
          }
        }
      });

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static String getServiceName(Type type, Program program) throws Exception {
    return String.format("%s.%s.%s.%s", type.name().toLowerCase(),
                         program.getAccountId(), program.getApplicationId(), type.name().toLowerCase());
  }

  public static Set<String> getServingHostNames(InputStream jarInputStream) throws Exception {
    try {
      Set<String> hostNames = Sets.newHashSet();
      JarInputStream jarInput = new JarInputStream(jarInputStream);

      JarEntry jarEntry;
      String webappDir = Constants.Webapp.WEBAPP_DIR + "/";
      while ((jarEntry = jarInput.getNextJarEntry()) != null) {
        if (jarEntry.getName().startsWith(webappDir) && !jarEntry.getName().equals(webappDir)) {
          String hostName = Iterables.get(Splitter.on('/').split(jarEntry.getName()), 1);
          hostNames.add(Networks.normalizeWebappDiscoveryName(hostName));
        }
      }

      return hostNames;
    } finally {
      jarInputStream.close();
    }
  }
}
