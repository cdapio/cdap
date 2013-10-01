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
import com.google.common.base.Charsets;
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
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLEncoder;
import java.util.jar.JarEntry;
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

      LOG.info("Initializing web app for app {} with jar {}", program.getApplicationId(),
               program.getJarLocation().getName());

      // Setup program jar for serving
      File baseDir = Files.createTempDir();
      File jarFile = new File(program.getJarLocation().toURI());
      int numFiles = JarExploder.explode(jarFile, baseDir, EXPLODE_FILTER);

      // Generate the service name using manifest information.
      String serviceName = getServiceName(jarFile, "webapp", program);
      Preconditions.checkNotNull(serviceName, "Cannot determine service name for program %s", program.getName());
      LOG.info("Got service name {}", serviceName);

      // Start netty server
      // TODO: add metrics reporting
      NettyHttpService.Builder builder = NettyHttpService.builder();
      builder.addHttpHandlers(ImmutableList.of(new WebappHandler(new File(baseDir, WEBAPP_DIR))));
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

  public static String getServiceName(File jarFile, String type, Program program) throws Exception {
    JarFile jar = new JarFile(jarFile);

    try {
      Manifest manifest = jar.getManifest();
      String host = manifest.getMainAttributes().getValue(ManifestFields.WEBAPP_HOST);
      host = normalizeHost(host);

      return String.format("%s.%s.%s.%s", type, program.getAccountId(), program.getApplicationId(), host);
    } finally {
      jar.close();
    }
  }

  /**
   * Removes ":80" from end of the host, replaces '.', '/' and '-' with '_' and URL encodes it.
   * @param host host that needs to be normalized.
   * @return the normalized host.
   */
  static String normalizeHost(String host) throws UnsupportedEncodingException {
    if (host.endsWith(":80")) {
      host = host.substring(0, host.length() - 3);
    }

    host = host.replace('.', '_');
    host = host.replace('-', '_');
    host = host.replace('/', '_');

    return URLEncoder.encode(host, Charsets.UTF_8.name());
  }

  private static final Predicate<JarEntry> EXPLODE_FILTER = new Predicate<JarEntry>() {
    @Override
    public boolean apply(JarEntry input) {
      return input.getName().startsWith(WEBAPP_DIR);
    }
  };
}
