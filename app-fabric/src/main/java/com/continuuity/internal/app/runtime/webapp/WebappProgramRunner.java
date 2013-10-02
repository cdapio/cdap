package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.app.program.ManifestFields;
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
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

/**
 * Run Webapp server.
 */
public class WebappProgramRunner implements ProgramRunner {
  private static final Logger LOG = LoggerFactory.getLogger(WebappProgramRunner.class);

  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;

  @Inject
  public WebappProgramRunner(ServiceAnnouncer serviceAnnouncer,
                             @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname) {
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;

  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    try {

      Type processorType = program.getType();
      Preconditions.checkNotNull(processorType, "Missing processor type.");
      Preconditions.checkArgument(processorType == Type.WEBAPP, "Only WEBAPP process type is supported.");

      LOG.info("Initializing web app for app {} with jar {}", program.getApplicationId(),
               program.getJarLocation().getName());

      // Setup program jar for serving
      File baseDir = Files.createTempDir();
      File jarFile = new File(program.getJarLocation().toURI());
      int numFiles = JarExploder.explode(jarFile, baseDir, EXPLODE_FILTER);

      // Generate the service name using manifest information.
      String serviceName = getServiceName(program.getJarLocation().getInputStream(),
                                          Type.WEBAPP.name().toLowerCase(), program);
      Preconditions.checkNotNull(serviceName, "Cannot determine service name for program %s", program.getName());
      LOG.info("Got service name {}", serviceName);

      // Start netty server
      // TODO: add metrics reporting
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableList.of(new WebappHandler(new File(baseDir, Constants.Webapp.WEBAPP_DIR))));
      builder.setHost(hostname.getCanonicalHostName());
      NettyHttpService httpService = builder.build();
      httpService.startAndWait();
      InetSocketAddress address = httpService.getBindAddress();

      RunId runId = RunIds.generate();
      LOG.info("Webapp running on address {} with {} files, and registering as {}", address, numFiles, serviceName);

      return new WebappProgramController(program.getName(), runId, httpService,
                                         serviceAnnouncer.announce(serviceName, address.getPort()));

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static String getServiceName(InputStream jarInputStream, String type, Program program) throws Exception {
    try {
      JarInputStream jarInput = new JarInputStream(jarInputStream);
      Manifest manifest = jarInput.getManifest();
      String host = manifest.getMainAttributes().getValue(ManifestFields.WEBAPP_HOST);
      host = Networks.normalizeHost(host);

      return String.format("%s.%s.%s.%s", type, program.getAccountId(), program.getApplicationId(), host);
    } finally {
      jarInputStream.close();
    }
  }

  private static final Predicate<JarEntry> EXPLODE_FILTER = new Predicate<JarEntry>() {
    @Override
    public boolean apply(JarEntry input) {
      return input.getName().startsWith(Constants.Webapp.WEBAPP_DIR);
    }
  };
}
