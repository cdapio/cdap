/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.security.impersonation;


import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.kerberos.DefaultOwnerAdmin;
import co.cask.cdap.common.kerberos.ImpersonatedOpType;
import co.cask.cdap.common.kerberos.ImpersonationRequest;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.kerberos.OwnerStore;
import co.cask.cdap.common.kerberos.PrincipalCredentials;
import co.cask.cdap.common.kerberos.UGIWithPrincipal;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.codec.NamespacedEntityIdCodec;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
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
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
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
  //  private static File keytabFile;
  private static InMemoryNamespaceClient namespaceClient;
  private static KerberosPrincipalId aliceKerberosPrincipalId;
  private static KerberosPrincipalId bobKerberosPrincipalId;

  private static File keytabDirPath;
  private static File aliceKeytabFile;
  private static File bobKeytabFile;

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

    namespaceClient = new InMemoryNamespaceClient();

    // Start KDC
    miniKdc = new MiniKdc(MiniKdc.createConf(), TEMP_FOLDER.newFolder());
    miniKdc.start();
    System.setProperty("java.security.krb5.conf", miniKdc.getKrb5conf().getAbsolutePath());

    keytabDirPath = TEMP_FOLDER.newFolder();
    // Generate keytab
    aliceKeytabFile = createPrincipal(keytabDirPath, "alice");
    bobKeytabFile = createPrincipal(keytabDirPath, "bob");

    // construct Kerberos PrincipalIds
    aliceKerberosPrincipalId = new KerberosPrincipalId(getPrincipal("alice"));
    bobKerberosPrincipalId = new KerberosPrincipalId(getPrincipal("bob"));

    // Start mini DFS cluster
    Configuration hConf = new Configuration();
    hConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    hConf.setBoolean("ipc.client.fallback-to-simple-auth-allowed", true);

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
  public void testDefaultUGIProviderWithLocalFiles() throws Exception {
    System.setProperty("sun.security.krb5.debug", "true");

    // sets the path of local keytabs in cConf to be used by SecurityUtil to fetch owner keytab
    setKeytabDir(keytabDirPath.getAbsolutePath());

    OwnerAdmin ownerAdmin = getOwnerAdmin();

    DefaultUGIProvider provider = new DefaultUGIProvider(cConf, locationFactory, ownerAdmin, namespaceClient);

    // add an owner for stream and dataset
    StreamId aliceEntity = new StreamId("ugiProviderNs", "dummyStream");
    namespaceClient.create(new NamespaceMeta.Builder().setName("ugiProviderNs").build());
    ownerAdmin.add(aliceEntity, aliceKerberosPrincipalId);
    DatasetId bobEntity = new DatasetId("ugiProviderNs", "dummyDataset");
    ownerAdmin.add(bobEntity, bobKerberosPrincipalId);

    // Try with local keytab file
    ImpersonationRequest aliceImpRequest = new ImpersonationRequest(aliceEntity, ImpersonatedOpType.OTHER);
    UGIWithPrincipal aliceUGIWithPrincipal = provider.getConfiguredUGI(aliceImpRequest);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
                        aliceUGIWithPrincipal.getUGI().getAuthenticationMethod());
    Assert.assertEquals(aliceKerberosPrincipalId.getPrincipal(), aliceUGIWithPrincipal.getPrincipal());
    Assert.assertTrue(aliceUGIWithPrincipal.getUGI().hasKerberosCredentials());

    // Fetch it again, it is should return the same UGI since there is caching
    Assert.assertSame(aliceUGIWithPrincipal.getUGI(), provider.getConfiguredUGI(aliceImpRequest).getUGI());

    ImpersonationRequest bobImpRequest = new ImpersonationRequest(bobEntity, ImpersonatedOpType.OTHER);
    UGIWithPrincipal bobUGIWithPrincipal = provider.getConfiguredUGI(bobImpRequest);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
                        bobUGIWithPrincipal.getUGI().getAuthenticationMethod());
    Assert.assertTrue(bobUGIWithPrincipal.getUGI().hasKerberosCredentials());
    Assert.assertEquals(bobKerberosPrincipalId.getPrincipal(), bobUGIWithPrincipal.getPrincipal());

    Assert.assertTrue(bobKeytabFile.delete());

    // Fetch the bob UGI again, it should still return the valid one
    Assert.assertSame(bobUGIWithPrincipal, provider.getConfiguredUGI(bobImpRequest));

    // Invalid the cache, getting of Alice UGI should pass, while getting of Bob should fails
    provider.invalidCache();
    Assert.assertNotSame(aliceUGIWithPrincipal, provider.getConfiguredUGI(aliceImpRequest));
    try {
      provider.getConfiguredUGI(bobImpRequest);
      Assert.fail("Expected IllegalArgumentException when getting UGI for " + bobUGIWithPrincipal);
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  @Test
  public void testDefaultUGIProviderWithHDFSFiles() throws Exception {
    Location hdfsKeytabDir = locationFactory.create("keytabs");
    setKeytabDir(hdfsKeytabDir.toURI().toString());

    Location aliceRemoteKeytabFile = hdfsKeytabDir.append("alice.keytab");
    aliceRemoteKeytabFile.createNew();
    Files.copy(aliceKeytabFile, Locations.newOutputSupplier(aliceRemoteKeytabFile));

    Location bobRemoteKeytabFile = hdfsKeytabDir.append("bob.keytab");
    bobRemoteKeytabFile.createNew();
    Files.copy(bobKeytabFile, Locations.newOutputSupplier(bobRemoteKeytabFile));

    OwnerAdmin ownerAdmin = getOwnerAdmin();

    // add an owner for stream and dataset
    StreamId aliceEntity = new StreamId("ugiProviderNs", "dummyStream");
    namespaceClient.create(new NamespaceMeta.Builder().setName("ugiProviderNs").build());
    ownerAdmin.add(aliceEntity, aliceKerberosPrincipalId);
    DatasetId bobEntity = new DatasetId("ugiProviderNs", "dummyDataset");
    ownerAdmin.add(bobEntity, bobKerberosPrincipalId);

    DefaultUGIProvider provider = new DefaultUGIProvider(cConf, locationFactory, ownerAdmin, namespaceClient);

    ImpersonationRequest aliceImpRequest = new ImpersonationRequest(aliceEntity, ImpersonatedOpType.OTHER);
    UGIWithPrincipal aliceUGIWithPrincipal = provider.getConfiguredUGI(aliceImpRequest);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
                        aliceUGIWithPrincipal.getUGI().getAuthenticationMethod());
    Assert.assertTrue(aliceUGIWithPrincipal.getUGI().hasKerberosCredentials());
    Assert.assertEquals(aliceKerberosPrincipalId.getPrincipal(), aliceUGIWithPrincipal.getPrincipal());

    // Fetch it again, it is should return the same UGI since there is caching
    Assert.assertSame(aliceUGIWithPrincipal.getUGI(), provider.getConfiguredUGI(aliceImpRequest).getUGI());

    ImpersonationRequest bobImpRequest = new ImpersonationRequest(bobEntity, ImpersonatedOpType.OTHER);
    UGIWithPrincipal bobUGIWithPrincipal = provider.getConfiguredUGI(bobImpRequest);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
                        bobUGIWithPrincipal.getUGI().getAuthenticationMethod());
    Assert.assertTrue(bobUGIWithPrincipal.getUGI().hasKerberosCredentials());
    Assert.assertEquals(bobKerberosPrincipalId.getPrincipal(), bobUGIWithPrincipal.getPrincipal());

    Assert.assertTrue(bobRemoteKeytabFile.delete());

    // Fetch the bob UGI again, it should still return the valid one
    Assert.assertSame(bobUGIWithPrincipal, provider.getConfiguredUGI(bobImpRequest));

    // Invalid the cache, getting of Alice UGI should pass, while getting of Bob should fails
    provider.invalidCache();
    Assert.assertNotSame(aliceUGIWithPrincipal, provider.getConfiguredUGI(aliceImpRequest));
    try {
      provider.getConfiguredUGI(bobImpRequest);
      Assert.fail("Expected IOException when getting UGI for " + bobImpRequest);
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

    setKeytabDir(keytabDirPath.getAbsolutePath());

    OwnerAdmin ownerAdmin = getOwnerAdmin();

    // add an owner for stream
    StreamId aliceEntity = new StreamId("ugiProviderNs", "dummyStream");
    namespaceClient.create(new NamespaceMeta.Builder().setName("ugiProviderNs").build());
    ownerAdmin.add(aliceEntity, aliceKerberosPrincipalId);


    try {
      InMemoryDiscoveryService discoveryService = new InMemoryDiscoveryService();
      discoveryService.register(new Discoverable(Constants.Service.APP_FABRIC_HTTP, httpService.getBindAddress()));

      // Create Alice UGI
      RemoteUGIProvider ugiProvider = new RemoteUGIProvider(cConf, discoveryService, locationFactory);

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
      httpService.stopAndWait();
    }
  }

  private OwnerAdmin getOwnerAdmin() {
    return new DefaultOwnerAdmin(cConf, new MockOwnerStore(), namespaceClient);
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
  private static final class UGIProviderTestHandler extends AbstractHttpHandler {

    private static final Gson GSON = new GsonBuilder()
      .registerTypeAdapter(NamespacedEntityId.class, new NamespacedEntityIdCodec())
      .create();

    @Path("/v1/impersonation/credentials")
    @POST
    public void getCredentials(HttpRequest request, HttpResponder responder) throws IOException {
      ImpersonationRequest impersonationRequest =
        GSON.fromJson(request.getContent().toString(StandardCharsets.UTF_8), ImpersonationRequest.class);
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
      responder.sendJson(HttpResponseStatus.OK, principalCredentials);
    }
  }

  private static class MockOwnerStore implements OwnerStore {

    final Map<NamespacedEntityId, KerberosPrincipalId> ownerInfo = new HashMap<>();

    @Override
    public void add(NamespacedEntityId entityId,
                    KerberosPrincipalId kerberosPrincipalId) throws IOException, AlreadyExistsException {
      ownerInfo.put(entityId, kerberosPrincipalId);
    }

    @Nullable
    @Override
    public KerberosPrincipalId getOwner(NamespacedEntityId entityId) throws IOException {
      return ownerInfo.get(entityId);
    }

    @Override
    public boolean exists(NamespacedEntityId entityId) throws IOException {
      return ownerInfo.containsKey(entityId);
    }

    @Override
    public void delete(NamespacedEntityId entityId) throws IOException {
      ownerInfo.remove(entityId);
    }
  }
}
