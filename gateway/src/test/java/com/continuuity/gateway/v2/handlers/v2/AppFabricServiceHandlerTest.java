package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.api.Application;
import com.continuuity.app.program.ManifestFields;
import com.continuuity.gateway.GatewayFastTestsSuite;
import com.continuuity.gateway.apps.wordcount.WordCount;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import junit.framework.Assert;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * Testing of App Fabric REST Endpoints.
 */
public class AppFabricServiceHandlerTest {

  /**
   * Deploys and application.
   */
  private HttpResponse deploy(Class<? extends Application> application) throws Exception {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(ManifestFields.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(ManifestFields.MAIN_CLASS, application.getName());

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    final JarOutputStream jarOut = new JarOutputStream(bos, manifest);
    final String pkgName = application.getPackage().getName();

    // Grab every classes under the application class package.
    try {
      Dependencies.findClassDependencies(application.getClassLoader(), new Dependencies.ClassAcceptor() {
        @Override
        public boolean accept(String className, URL classUrl, URL classPathUrl) {
          try {
            if (className.startsWith(pkgName)) {
              jarOut.putNextEntry(new JarEntry(className.replace('.', '/') + ".class"));
              InputStream in = classUrl.openStream();
              try {
                ByteStreams.copy(in, jarOut);
              } finally {
                in.close();
              }
              return true;
            }
            return false;
          } catch (Exception e) {
            throw Throwables.propagate(e);
          }
        }
      }, application.getName());
    } finally {
      jarOut.close();
    }

    HttpPut put = GatewayFastTestsSuite.getPUT("/v2/apps");
    put.setHeader(GatewayAuthenticator.CONTINUUITY_API_KEY, "api-key-example");
    put.setHeader("X-Archive-Name", application.getSimpleName() + ".jar");
    put.setEntity(new ByteArrayEntity(bos.toByteArray()));
    return GatewayFastTestsSuite.PUT(put);
  }

  /**
   * Tests deploying an application.
   */
  @Test
  public void testDeploy() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
  }

  /**
   * Tests deleting an application.
   */
  @Test
  public void testDeleteApp() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps/WordCount").getStatusLine().getStatusCode());
  }

  /**
   * Test deleting of all applications.
   */
  @Test
  public void testDeleteAllApps() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps").getStatusLine().getStatusCode());
  }

  /**
   * @return Runnable status
   */
  private String getRunnableStatus(String runnableType, String appId, String runnableId) throws Exception {
    HttpResponse response =
      GatewayFastTestsSuite.GET("/v2/apps/" + appId + "/" + runnableType + "/" + runnableId + "/status");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String s = EntityUtils.toString(response.getEntity());
    Map<String, String> o = new Gson().fromJson(s, new TypeToken<Map<String, String>>() {}.getType());
    return o.get("status");
  }

  /**
   * Tests deploying a flow, starting a flow, stopping a flow and deleting the application.
   */
  @Test
  public void testStartStopStatusOfFlow() throws Exception {
    HttpResponse response = deploy(WordCount.class);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    try {
      Assert.assertEquals(200,
             GatewayFastTestsSuite.POST("/v2/apps/WordCount/flows/WordCounter/start", null)
               .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("RUNNING", getRunnableStatus("flows", "WordCount", "WordCounter"));
    } finally {
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.POST("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode()
      );
      Assert.assertEquals("STOPPED", getRunnableStatus("flows", "WordCount", "WordCounter"));
      Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps/WordCount").getStatusLine().getStatusCode());
      Assert.assertEquals(404, GatewayFastTestsSuite.DELETE("/v2/apps/WordCount").getStatusLine().getStatusCode());
    }
  }

  /**
   * Tests history of a flow
   */
  @Test
  public void testFlowHistory() throws Exception {
    try {
      HttpResponse response = deploy(WordCount.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.POST("/v2/apps/WordCount/flows/WordCounter/start", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.POST("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.POST("/v2/apps/WordCount/flows/WordCounter/start", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200,
                          GatewayFastTestsSuite.POST("/v2/apps/WordCount/flows/WordCounter/stop", null)
                            .getStatusLine().getStatusCode());
      Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps/WordCount").getStatusLine().getStatusCode());

      response = GatewayFastTestsSuite.GET("/v2/apps/WordCount/flows/WordCounter/history");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      List<Map<String, String>> o = new Gson().fromJson(s, new TypeToken<List<Map<String, String>>>(){}.getType());

      // We started and stopped twice, so we should have 2 entries.
      Assert.assertTrue(o.size() >= 2);

      // For each one, we have 4 fields.
      for (Map<String, String> m : o) {
        Assert.assertEquals(4, m.size());
      }
    } finally {
      Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  @Test
  public void testSetGetFlowletInstances() throws Exception {

  }

  /**
   * Tests specification API for a flow.
   */
  @Test
  public void testRunnableSpecification() throws Exception {
    try {
      HttpResponse response = deploy(WordCount.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      response = GatewayFastTestsSuite.GET("/v2/apps/WordCount/flows/WordCounter");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      String s = EntityUtils.toString(response.getEntity());
      Assert.assertNotNull(s);
    } finally {
      Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps").getStatusLine().getStatusCode());
    }
  }

  /**
   * Test for resetting app.
   */
  @Test
  public void testUnRecoverableReset() throws Exception {
    try {
      HttpResponse response = deploy(WordCount.class);
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
      response = GatewayFastTestsSuite.DELETE("/v2/unrecoverable/reset");
      Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    } finally {
      Assert.assertEquals(200, GatewayFastTestsSuite.DELETE("/v2/apps").getStatusLine().getStatusCode());
    }
  }
}
