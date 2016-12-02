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

package co.cask.cdap.common.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.InMemoryDiscoveryService;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Unit tests for {@link UGIProvider}.
 */
public class UGIProviderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static CConfiguration cConf;
  private static MiniDFSCluster miniDFSCluster;
  private static LocationFactory locationFactory;
  private static MiniKdc miniKdc;
  private static File keytabFile;

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    // Start KDC
    miniKdc = new MiniKdc(MiniKdc.createConf(), TEMP_FOLDER.newFolder());
    miniKdc.start();
    System.setProperty("java.security.krb5.conf", miniKdc.getKrb5conf().getAbsolutePath());

    // Generate keytab
    keytabFile = TEMP_FOLDER.newFile();
    miniKdc.createPrincipal(keytabFile, "hdfs", "alice", "bob");

    // Start mini DFS cluster
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    miniDFSCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    miniDFSCluster.waitClusterUp();
    locationFactory = new FileContextLocationFactory(miniDFSCluster.getFileSystem().getConf());

    hConf = new Configuration();
    hConf.set("hadoop.security.authentication", "kerberos");
    UserGroupInformation.setConfiguration(hConf);
  }

  @AfterClass
  public static void finish() {
    if (miniDFSCluster != null) {
      miniDFSCluster.shutdown();
    }
    if (miniKdc != null) {
      miniKdc.stop();
    }
  }

  @Test
  public void testDefaultUGIProvider() throws IOException {
    System.setProperty("sun.security.krb5.debug", "true");

    DefaultUGIProvider provider = new DefaultUGIProvider(cConf, locationFactory);

    // Try with local keytab file
    ImpersonationInfo aliceInfo = new ImpersonationInfo(getPrincipal("alice"), keytabFile.getAbsolutePath());
    UserGroupInformation aliceUGI = provider.getConfiguredUGI(aliceInfo);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, aliceUGI.getAuthenticationMethod());
    Assert.assertTrue(aliceUGI.hasKerberosCredentials());

    // Fetch it again, it is should return the same UGI since there is caching
    Assert.assertSame(aliceUGI, provider.getConfiguredUGI(aliceInfo));

    // Put the keytab on HDFS
    Location remoteKeytab = locationFactory.create("keytab").getTempFile(".tmp");
    Files.copy(keytabFile, Locations.newOutputSupplier(remoteKeytab));

    // Login with remote keytab file
    ImpersonationInfo bobInfo = new ImpersonationInfo(getPrincipal("bob"), remoteKeytab.toURI().toString());
    UserGroupInformation bobUGI = provider.getConfiguredUGI(bobInfo);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS, bobUGI.getAuthenticationMethod());
    Assert.assertTrue(bobUGI.hasKerberosCredentials());

    // Delete the keytab on HDFS
    remoteKeytab.delete();

    // Fetch the bob UGI again, it should still return the valid one
    Assert.assertSame(bobUGI, provider.getConfiguredUGI(bobInfo));

    // Invalid the cache, getting of Alice UGI should pass, while getting of Bob should fails
    provider.invalidCache();
    Assert.assertNotSame(aliceUGI, provider.getConfiguredUGI(aliceInfo));
    try {
      provider.getConfiguredUGI(bobInfo);
      Assert.fail("Expected IOException when getting UGI for " + bobInfo);
    } catch (IOException e) {
      // Expected
    }
  }

  @Test
  public void testRemoteUGIProvider() throws Exception {
    // Starts a mock server to handle remote UGI requests
    final NettyHttpService httpService = NettyHttpService.builder("remoteUGITest")
      .addHttpHandlers(Collections.singleton(new UGIProviderTestHandler()))
      .build();

    httpService.startAndWait();
    try {
      InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
      discoveryService.register(new Discoverable(Constants.Service.APP_FABRIC_HTTP, httpService.getBindAddress()));

      // Create Alice UGI
      RemoteUGIProvider ugiProvider = new RemoteUGIProvider(cConf, discoveryService, locationFactory);
      ImpersonationInfo aliceInfo = new ImpersonationInfo(getPrincipal("alice"), keytabFile.toURI().toString());
      UserGroupInformation aliceUGI = ugiProvider.getConfiguredUGI(aliceInfo);

      // Shouldn't be a kerberos UGI
      Assert.assertFalse(aliceUGI.hasKerberosCredentials());
      // Validate the credentials
      Token<? extends TokenIdentifier> token = aliceUGI.getCredentials().getToken(new Text("principal"));
      Assert.assertArrayEquals(aliceInfo.getPrincipal().getBytes(StandardCharsets.UTF_8), token.getIdentifier());
      Assert.assertArrayEquals(aliceInfo.getPrincipal().getBytes(StandardCharsets.UTF_8), token.getPassword());
      Assert.assertEquals(new Text("principal"), token.getKind());
      Assert.assertEquals(new Text("service"), token.getService());

      token = aliceUGI.getCredentials().getToken(new Text("keytab"));
      Assert.assertArrayEquals(aliceInfo.getKeytabURI().getBytes(StandardCharsets.UTF_8), token.getIdentifier());
      Assert.assertArrayEquals(aliceInfo.getKeytabURI().getBytes(StandardCharsets.UTF_8), token.getPassword());
      Assert.assertEquals(new Text("keytab"), token.getKind());
      Assert.assertEquals(new Text("service"), token.getService());

      // Fetch it again, it should return the same UGI due to caching
      Assert.assertSame(aliceUGI, ugiProvider.getConfiguredUGI(aliceInfo));

      // Invalid the cache and fetch it again. A different UGI should be returned
      ugiProvider.invalidCache();
      Assert.assertNotSame(aliceUGI, ugiProvider.getConfiguredUGI(aliceInfo));

    } finally {
      httpService.stopAndWait();
    }
  }

  private static String getPrincipal(String name) {
    return String.format("%s@%s", name, miniKdc.getRealm());
  }

  /**
   * A http handler to provide the "/v1/impersonation/credentials" endpoint for
   * testing of the {@link RemoteUGIProvider}.
   */
  public static final class UGIProviderTestHandler extends AbstractHttpHandler {

    @Path("/v1/impersonation/credentials")
    @POST
    public void getCredentials(HttpRequest request, HttpResponder responder) throws IOException {
      ImpersonationInfo impersonationInfo = new Gson().fromJson(request.getContent().toString(StandardCharsets.UTF_8),
                                                                ImpersonationInfo.class);
      // Generate a Credentials based on the request info
      Credentials credentials = new Credentials();
      credentials.addToken(new Text("principal"),
                           new Token<>(impersonationInfo.getPrincipal().getBytes(StandardCharsets.UTF_8),
                                       impersonationInfo.getPrincipal().getBytes(StandardCharsets.UTF_8),
                                       new Text("principal"),
                                       new Text("service")));
      credentials.addToken(new Text("keytab"),
                           new Token<>(impersonationInfo.getKeytabURI().getBytes(StandardCharsets.UTF_8),
                                       impersonationInfo.getKeytabURI().getBytes(StandardCharsets.UTF_8),
                                       new Text("keytab"),
                                       new Text("service")));

      // Write it to HDFS
      Location credentialsDir = locationFactory.create("credentials");
      Preconditions.checkState(credentialsDir.mkdirs());

      Location credentialsFile = credentialsDir.append("tmp").getTempFile(".credentials");
      try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(credentialsFile.getOutputStream()))) {
        credentials.writeTokenStorageToStream(os);
      }
      responder.sendString(HttpResponseStatus.OK, credentialsFile.toURI().toString());
    }
  }
}
