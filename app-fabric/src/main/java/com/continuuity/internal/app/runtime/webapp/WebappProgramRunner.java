package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.NettyHttpService;
import com.continuuity.weave.api.RunId;
import com.continuuity.weave.api.ServiceAnnouncer;
import com.continuuity.weave.internal.RunIds;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static com.continuuity.common.conf.Constants.Webapp.WEBAPP_DIR;

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

      LOG.info("Initializing web app for program {} with jar {}", program.getName(), program.getJarLocation());

      // Setup program jar for serving
      File baseDir = Files.createTempDir();
      File jarFile = new File(program.getJarLocation().toURI());
      int numFiles = JarExploder.explode(jarFile, baseDir, WEBAPP_DIR);
      String serviceName = getServiceName(jarFile);
      Preconditions.checkNotNull(serviceName, "Cannot determine service name for program %s", program.getName());

      // Start netty server
      // TODO: add metrics reporting
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableList.of(new WebappHandler(new File(baseDir, WEBAPP_DIR))));
      builder.setHost(hostname.getCanonicalHostName());
      NettyHttpService httpService = builder.build();
      httpService.startAndWait();
      InetSocketAddress address = httpService.getBindAddress();

      RunId runId = RunIds.generate();
      LOG.info("Using runid {}", runId);
      LOG.info("Webapp running on address {} with {} files", address, numFiles);

      return new WebappProgramController(program.getName(), runId, httpService,
                                         serviceAnnouncer.announce(normalizeHost(serviceName), address.getPort()));

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getServiceName(File jarFile) throws Exception {
    JarFile jar = new JarFile(jarFile);

    try {
      Manifest manifest = jar.getManifest();
      return manifest.getMainAttributes().getValue(ManifestFields.WEBAPP_HOST);
    } finally {
      jar.close();
    }
  }

  /**
   * Removes "www." from beginning and ":80" from end of the host.
   * @param host host that needs to be normalized.
   * @return the shortened host.
   */
  static String normalizeHost(String host) {
    if (host.startsWith("www.")) {
      host = host.substring(4);
    }

    if (host.endsWith(":80")) {
      host = host.substring(0, host.length() - 3);
    }

    return host;
  }
}
