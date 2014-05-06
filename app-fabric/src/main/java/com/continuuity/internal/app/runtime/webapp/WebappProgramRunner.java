package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.Networks;
import com.continuuity.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.internal.RunIds;
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
  private final WebappHttpHandlerFactory webappHttpHandlerFactory;

  @Inject
  public WebappProgramRunner(ServiceAnnouncer serviceAnnouncer, DiscoveryService discoveryService,
                             @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                             WebappHttpHandlerFactory webappHttpHandlerFactory) {
    this.serviceAnnouncer = serviceAnnouncer;
    this.discoveryService = discoveryService;
    this.hostname = hostname;
    this.webappHttpHandlerFactory = webappHttpHandlerFactory;
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
      JarHttpHandler jarHttpHandler = webappHttpHandlerFactory.createHandler(program.getJarLocation());
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableSet.of(jarHttpHandler));
      builder.setUrlRewriter(new WebappURLRewriter(jarHttpHandler));
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

  private static final String DEFAULT_DIR_NAME_COLON = ServePathGenerator.DEFAULT_DIR_NAME + ":";

  public static Set<String> getServingHostNames(InputStream jarInputStream) throws Exception {
    try {
      Set<String> hostNames = Sets.newHashSet();
      JarInputStream jarInput = new JarInputStream(jarInputStream);

      JarEntry jarEntry;
      String webappDir = Constants.Webapp.WEBAPP_DIR + "/";
      while ((jarEntry = jarInput.getNextJarEntry()) != null) {
        if (!jarEntry.isDirectory() && jarEntry.getName().startsWith(webappDir) &&
          jarEntry.getName().contains(ServePathGenerator.SRC_PATH)) {
          // Format is - webapp/host:port/[path/]src/files
          String webappHostName = Iterables.get(Splitter.on("/src/").split(jarEntry.getName()), 0);
          String hostName = Iterables.get(Splitter.on('/').limit(2).split(webappHostName), 1);

          hostNames.add(hostName);
        }
      }

      Set<String> registerNames = Sets.newHashSetWithExpectedSize(hostNames.size());
      for (String hostName : hostNames) {
        if (hostName.equals(ServePathGenerator.DEFAULT_DIR_NAME)) {
          LOG.warn("Not registering default service name. Default service needs to have a routable path");
          continue;
        } else if (hostName.startsWith(DEFAULT_DIR_NAME_COLON)) {
          LOG.warn("Not registering default service name with explicit port - {}.", hostName);
          continue;
        }

        registerNames.add(Networks.normalizeWebappDiscoveryName(hostName));
      }

      return registerNames;
    } finally {
      jarInputStream.close();
    }
  }
}
