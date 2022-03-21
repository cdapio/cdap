/*
 * Copyright © 2016-2022 Cask Data, Inc.
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

package io.cdap.cdap.security.impersonation;


import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.DefaultInternalAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.proto.codec.EntityIdTypeAdapter;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.security.auth.context.AuthenticationTestContext;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.cdap.http.NettyHttpService;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
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
  private static InMemoryNamespaceAdmin namespaceClient;
  private static KerberosPrincipalId aliceKerberosPrincipalId;
  private static KerberosPrincipalId bobKerberosPrincipalId;
  private static KerberosPrincipalId eveKerberosPrincipalId;
  private static NamespaceId namespaceId = new NamespaceId("UGIProviderTest");
  private static DatasetId aliceEntity = namespaceId.dataset("aliceDataset");
  private static DatasetId bobEntity = namespaceId.dataset("dummyDataset");

  private static File localKeytabDirPath;
  private static File aliceKeytabFile;
  private static File bobKeytabFile;
  private static File eveKeytabFile;

  private static File createPrincipal(File keytabDirPath, String username) throws Exception {
    File keytabFile = new File(keytabDirPath, username + ".keytab");
    Assert.assertTrue(keytabFile.createNewFile());
    miniKdc.createPrincipal(keytabFile, username);
    return keytabFile;
  }

  @BeforeClass
  public static void init() throws Exception {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    namespaceClient = new InMemoryNamespaceAdmin();

    // Start KDC
    miniKdc = new MiniKdc(MiniKdc.createConf(), TEMP_FOLDER.newFolder());
    miniKdc.start();
    System.setProperty("java.security.krb5.conf", miniKdc.getKrb5conf().getAbsolutePath());

    localKeytabDirPath = TEMP_FOLDER.newFolder();

    // Generate keytab
    aliceKeytabFile = createPrincipal(localKeytabDirPath, "alice");
    bobKeytabFile = createPrincipal(localKeytabDirPath, "bob");
    eveKeytabFile = createPrincipal(localKeytabDirPath, "eve");

    // construct Kerberos PrincipalIds
    aliceKerberosPrincipalId = new KerberosPrincipalId(getPrincipal("alice"));
    bobKerberosPrincipalId = new KerberosPrincipalId(getPrincipal("bob"));
    eveKerberosPrincipalId = new KerberosPrincipalId(getPrincipal("eve"));

    // Start mini DFS cluster
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    hConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);
    hConf.setBoolean("ignore.secure.ports.for.testing", true);

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
  public void testRemoteUGIProvider() throws Exception {
    // Starts a mock server to handle remote UGI requests
    final NettyHttpService httpService = NettyHttpService.builder("remoteUGITest")
      .setHttpHandlers(new UGIProviderTestHandler())
      .build();

    httpService.start();

    setKeytabDir(localKeytabDirPath.getAbsolutePath());

    OwnerAdmin ownerAdmin = getOwnerAdmin();

    // add an owner for stream
    ownerAdmin.add(aliceEntity, aliceKerberosPrincipalId);

    try {
      InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
      discoveryService.register(new Discoverable(Constants.Service.APP_FABRIC_HTTP, httpService.getBindAddress()));

      RemoteClientFactory remoteClientFactory =
        new RemoteClientFactory(discoveryService, new DefaultInternalAuthenticator(new AuthenticationTestContext()));
      RemoteUGIProvider ugiProvider = new RemoteUGIProvider(cConf, locationFactory,
                                                            ownerAdmin,
                                                            remoteClientFactory);

      ImpersonationRequest aliceImpRequest = new ImpersonationRequest(aliceEntity, ImpersonatedOpType.OTHER);
      UGIWithPrincipal aliceUGIWithPrincipal = ugiProvider.getConfiguredUGI(aliceImpRequest);

      // Shouldn't be a kerberos UGI
      Assert.assertFalse(aliceUGIWithPrincipal.getUGI().hasKerberosCredentials());
      // Validate the credentials
      Token<? extends TokenIdentifier> token =
        aliceUGIWithPrincipal.getUGI().getCredentials().getToken(new Text("entity"));
      Assert.assertArrayEquals(aliceEntity.toString().getBytes(StandardCharsets.UTF_8), token.getIdentifier());
      Assert.assertArrayEquals(aliceEntity.toString().getBytes(StandardCharsets.UTF_8), token.getPassword());
      Assert.assertEquals(new Text("entity"), token.getKind());
      Assert.assertEquals(new Text("service"), token.getService());

      token = aliceUGIWithPrincipal.getUGI().getCredentials().getToken(new Text("opType"));
      Assert.assertArrayEquals(aliceImpRequest.getImpersonatedOpType().toString().getBytes(StandardCharsets.UTF_8),
                               token.getIdentifier());
      Assert.assertArrayEquals(aliceImpRequest.getImpersonatedOpType().toString().getBytes(StandardCharsets.UTF_8),
                               token.getPassword());
      Assert.assertEquals(new Text("opType"), token.getKind());
      Assert.assertEquals(new Text("service"), token.getService());

      // Fetch it again, it should return the same UGI due to caching
      Assert.assertSame(aliceUGIWithPrincipal, ugiProvider.getConfiguredUGI(aliceImpRequest));

      // Invalid the cache and fetch it again. A different UGI should be returned
      ugiProvider.invalidCache();
      Assert.assertNotSame(aliceUGIWithPrincipal, ugiProvider.getConfiguredUGI(aliceImpRequest));
    } finally {
      httpService.stop();
    }

    // cleanup
    ownerAdmin.delete(aliceEntity);
  }

  private OwnerAdmin getOwnerAdmin() {
    return new DefaultOwnerAdmin(cConf, new InMemoryOwnerStore(), namespaceClient);
  }

  private void setKeytabDir(String keytabDirPath) {
    cConf.set(Constants.Security.KEYTAB_PATH, keytabDirPath + "/" +
      Constants.USER_NAME_SPECIFIER + ".keytab");
  }

  private static String getPrincipal(String name) {
    return String.format("%s@%s", name, miniKdc.getRealm());
  }

  /**
   * A http handler to provide the "/v1/impersonation/credentials" endpoint for
   * testing of the {@link RemoteUGIProvider}.
   */
  public static final class UGIProviderTestHandler extends AbstractHttpHandler {

    private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(NamespacedEntityId.class, new EntityIdTypeAdapter())
      .create();

    @Path("/v1/impersonation/credentials")
    @POST
    public void getCredentials(FullHttpRequest request, HttpResponder responder) throws IOException {
      ImpersonationRequest impersonationRequest =
        GSON.fromJson(request.content().toString(StandardCharsets.UTF_8), ImpersonationRequest.class);
      // Generate a Credentials based on the request info
      Credentials credentials = new Credentials();
      credentials.addToken(new Text("entity"),
                           new Token<>(impersonationRequest.getEntityId().toString().getBytes(StandardCharsets.UTF_8),
                                       impersonationRequest.getEntityId().toString().getBytes(StandardCharsets.UTF_8),
                                       new Text("entity"),
                                       new Text("service")));
      credentials.addToken(new Text("opType"),
                           new Token<>(impersonationRequest.getImpersonatedOpType().toString()
                                         .getBytes(StandardCharsets.UTF_8),
                                       impersonationRequest.getImpersonatedOpType().toString()
                                         .getBytes(StandardCharsets.UTF_8),
                                       new Text("opType"),
                                       new Text("service")));

      // Write it to HDFS
      Location credentialsDir = locationFactory.create("credentials");
      if (!credentialsDir.exists()) {
        Preconditions.checkState(credentialsDir.mkdirs());
      }

      Location credentialsFile = credentialsDir.append("tmp").getTempFile(".credentials");
      try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(credentialsFile.getOutputStream()))) {
        credentials.writeTokenStorageToStream(os);
      }
      PrincipalCredentials principalCredentials = new PrincipalCredentials(aliceKerberosPrincipalId.getPrincipal(),
                                                                           credentialsFile.toURI().toString());
      responder.sendJson(HttpResponseStatus.OK, GSON.toJson(principalCredentials));
    }
  }
}
