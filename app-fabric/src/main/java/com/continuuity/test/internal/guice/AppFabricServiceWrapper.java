package com.continuuity.test.internal.guice;

import com.continuuity.api.Application;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.ProgramId;
import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.handlers.AppFabricHttpHandler;
import com.continuuity.http.BodyConsumer;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.app.BufferFileInputStream;
import com.continuuity.internal.app.Specifications;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;


/**
 *
 */
public class AppFabricServiceWrapper extends AppFabricService {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServiceWrapper.class);


  public HttpRequest createHtttpRequest() {
   return  new HttpRequest() {
      @Override
      public HttpMethod getMethod() {
        return null;
      }

      @Override
      public void setMethod(HttpMethod method) {

      }

      @Override
      public String getUri() {
        return null;
      }

      @Override
      public void setUri(String uri) {

      }

      @Override
      public String getHeader(String name) {
        return null;
      }

      @Override
      public List<String> getHeaders(String name) {
        return null;
      }

      @Override
      public List<Map.Entry<String, String>> getHeaders() {
        return null;
      }

      @Override
      public boolean containsHeader(String name) {
        return false;
      }

      @Override
      public Set<String> getHeaderNames() {
        return null;
      }

      @Override
      public HttpVersion getProtocolVersion() {
        return null;
      }

      @Override
      public void setProtocolVersion(HttpVersion version) {

      }

      @Override
      public ChannelBuffer getContent() {
        return null;
      }

      @Override
      public void setContent(ChannelBuffer content) {

      }

      @Override
      public void addHeader(String name, Object value) {

      }

      @Override
      public void setHeader(String name, Object value) {

      }

      @Override
      public void setHeader(String name, Iterable<?> values) {

      }

      @Override
      public void removeHeader(String name) {

      }

      @Override
      public void clearHeaders() {

      }

      @Override
      public long getContentLength() {
        return 0;
      }

      @Override
      public long getContentLength(long defaultValue) {
        return 0;
      }

      @Override
      public boolean isChunked() {
        return false;
      }

      @Override
      public void setChunked(boolean chunked) {

      }

      @Override
      public boolean isKeepAlive() {
        return false;
      }
    };


  }

  public static void startFlow(AppFabricHttpHandler httpHandler, String accountId, String appId, String flowId) {

    MockResponder responder = new MockResponder();
    String uri = "/v2/apps/"+appId + "/flows/+" + flowId + "/start";
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, uri);
    httpHandler.startProgram(request,responder,appId,"flows",flowId);
    Preconditions.checkArgument(responder.getStatus().getCode()==200, "start flow failed");
  }

  public static void setFlowletInstances(AppFabricHttpHandler httpHandler, String accountId, String applicationId,
                                         ProgramId flowId, String flowletName, int instances) {
    MockResponder responder = new MockResponder();
    String uri = "/v2/apps/"+ applicationId + "/flows/+" + flowId + "/flowlets/"+
      flowletName + "/instances/" + instances;
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.PUT, uri);
    httpHandler.setFlowletInstances(request,responder,applicationId,flowId,flowletName);
    Preconditions.checkArgument(responder.getStatus().getCode()==200, "start flow failed");
  }


  private static final class MockResponder implements HttpResponder {
    private HttpResponseStatus status = null;

    HttpResponseStatus getStatus() {
      return status;
    }

    @Override
    public void sendJson(HttpResponseStatus status, Object object) {
      this.status = status;
    }

    @Override
    public void sendJson(HttpResponseStatus status, Object object, Type type) {

    }

    @Override
    public void sendJson(HttpResponseStatus status, Object object, Type type, Gson gson) {

    }

    @Override
    public void sendString(HttpResponseStatus status, String data) {
      this.status =status;
    }

    @Override
    public void sendStatus(HttpResponseStatus status) {

    }

    @Override
    public void sendStatus(HttpResponseStatus status, Multimap<String, String> headers) {

    }

    @Override
    public void sendByteArray(HttpResponseStatus status, byte[] bytes, Multimap<String, String> headers) {

    }

    @Override
    public void sendBytes(HttpResponseStatus status, ByteBuffer buffer, Multimap<String, String> headers) {

    }

    @Override
    public void sendError(HttpResponseStatus status, String errorMessage) {
      this.status = status;
    }

    @Override
    public void sendChunkStart(HttpResponseStatus status, Multimap<String, String> headers) {

    }

    @Override
    public void sendChunk(ChannelBuffer content) {

    }

    @Override
    public void sendChunkEnd() {

    }

    @Override
    public void sendContent(HttpResponseStatus status, ChannelBuffer content, String contentType, Multimap<String, String> headers) {

    }

    @Override
    public void sendFile(File file, Multimap<String, String> headers) {

    }
  }

  public static Location deployApplication(AppFabricHttpHandler httpHandler,
                                    LocationFactory locationFactory,
                                    final String account,
                                    final AuthToken token,
                                    final String applicationId,
                                    final String fileName,
                                    Class<? extends Application> applicationClz,
                                    File...bundleEmbeddedJars) throws Exception {

    Preconditions.checkNotNull(applicationClz, "Application cannot be null.");

    Application application = applicationClz.newInstance();
    ApplicationSpecification appSpec = Specifications.from(application.configure());
    Location deployedJar = locationFactory.create(createDeploymentJar(applicationClz, appSpec, bundleEmbeddedJars).toURI());
    LOG.info("Created deployedJar at {}", deployedJar.toURI().toASCIIString());

    ArchiveInfo archiveInfo = new ArchiveInfo(account, fileName);
    archiveInfo.setApplicationId(applicationId);
    String deployUri = "/v2/apps/";
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/v2/apps");
    request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, "api-key-example");
    request.setHeader("X-Archive-Name", applicationId + ".jar");
    MockResponder mockResponder = new MockResponder();
    BodyConsumer bodyConsumer = httpHandler.deploy(request, mockResponder);

    BufferFileInputStream is = new BufferFileInputStream(deployedJar.getInputStream(), 100 * 1024);
    try {
      byte[] chunk = is.read();
      while (chunk.length > 0) {
        mockResponder = new MockResponder();
        bodyConsumer.chunk(ChannelBuffers.wrappedBuffer(chunk), mockResponder);
        Preconditions.checkState(mockResponder.getStatus() == null, "failed to deploy app");
        chunk = is.read();
      }
      mockResponder = new MockResponder();
      bodyConsumer.finished(mockResponder);
      Preconditions.checkState(mockResponder.getStatus().getCode() == 200, "failed to deploy app");
      is.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
    }
    return deployedJar;
  }


  private static File createDeploymentJar(Class<?> clz, ApplicationSpecification appSpec, File...bundleEmbeddedJars) {
    File testAppDir;
    File tmpDir;
    testAppDir = Files.createTempDir();

    File outputDir = new File(testAppDir, "app");
    tmpDir = new File(testAppDir, "tmp");

    outputDir.mkdirs();
    tmpDir.mkdirs();

    // Creates Manifest
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, clz.getName());

    ClassLoader loader = clz.getClassLoader();
    Preconditions.checkArgument(loader != null, "Cannot get ClassLoader for class " + clz);
    String classFile = clz.getName().replace('.', '/') + ".class";

    // for easier testing within IDE we pick jar file first, before making this publicly available
    // we need to add code here to throw an exception if the class is in classpath twice (file and jar)
    // see ENG-2961
    try {
      // first look for jar file (in classpath) that contains class and return it
      URI fileUri = null;
      for (Enumeration<URL> itr = loader.getResources(classFile); itr.hasMoreElements(); ) {
        URI uri = itr.nextElement().toURI();
        if (uri.getScheme().equals("jar")) {
          String rawSchemeSpecificPart = uri.getRawSchemeSpecificPart();
          if (rawSchemeSpecificPart.startsWith("file:") && rawSchemeSpecificPart.contains("!")) {
            String[] parts = rawSchemeSpecificPart.substring("file:".length()).split("!");
            return new File(parts[0]);
          } else {
            return new File(uri.getPath());
          }
        } else if (uri.getScheme().equals("file")) {
          // memorize file URI in case there is no jar that contains the class
          fileUri = uri;
        }
      }
      if (fileUri != null) {
        // build jar file based on class file and return it
        File baseDir = new File(fileUri).getParentFile();

        Package appPackage = clz.getPackage();
        String packagePath = appPackage == null ? "" : appPackage.getName().replace('.', '/');
        String basePath = baseDir.getAbsolutePath();
        File relativeBase = new File(basePath.substring(0, basePath.length() - packagePath.length()));
        File jarFile = File.createTempFile(String.format("%s-%d", clz.getSimpleName(), System.currentTimeMillis()),
                                           ".jar", tmpDir);
        return jarDir(baseDir, relativeBase, manifest, jarFile, appSpec, bundleEmbeddedJars);
      } else {
        // return null if neither existing jar was found nor jar was built based on class file
        return null;
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  private static File jarDir(File dir, File relativeBase, Manifest manifest, File outputFile,
                             ApplicationSpecification appSpec, File...bundleEmbeddedJars)
    throws IOException, ClassNotFoundException {

    JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(outputFile), manifest);
    Queue<File> queue = Lists.newLinkedList();
    Collections.addAll(queue, dir.listFiles());

    URI basePath = relativeBase.toURI();
    while (!queue.isEmpty()) {
      File file = queue.remove();
      String entryName = basePath.relativize(file.toURI()).toString();
      jarOut.putNextEntry(new JarEntry(entryName));

      if (file.isFile()) {
        Files.copy(file, jarOut);
      } else {
        Collections.addAll(queue, file.listFiles());
      }
      jarOut.closeEntry();
    }

    for (File bundledEmbeddedJar : bundleEmbeddedJars) {
      String entryName = bundledEmbeddedJar.getName();
      jarOut.putNextEntry(new JarEntry(entryName));
      Files.copy(bundledEmbeddedJar, jarOut);
      jarOut.closeEntry();
    }

    jarOut.close();

    return outputFile;
  }


}

