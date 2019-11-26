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

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.InMemoryNamespaceClient;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.internal.app.store.DefaultStore;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.codec.EntityIdTypeAdapter;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.KerberosPrincipalId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import co.cask.http.NettyHttpService;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Injector;
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
public class DefaultUGIProviderTest extends AppFabricTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static CConfiguration cConf;
  private static MiniDFSCluster miniDFSCluster;
  private static LocationFactory locationFactory;
  private static MiniKdc miniKdc;
  private static InMemoryNamespaceClient namespaceClient;
  private static KerberosPrincipalId aliceKerberosPrincipalId;
  private static KerberosPrincipalId bobKerberosPrincipalId;
  private static KerberosPrincipalId eveKerberosPrincipalId;
  private static NamespaceId namespaceId = new NamespaceId("DefaultUGIProviderTest");
  private static StreamId aliceEntity = namespaceId.stream("dummyStream");
  private static DatasetId bobEntity = namespaceId.dataset("dummyDataset");

  private static File localKeytabDirPath;
  private static File aliceKeytabFile;
  private static File bobKeytabFile;
  private static File eveKeytabFile;
  private static Store store;
  private static ProgramRuntimeService runtimeService;;

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

    miniDFSCluster = new MiniDFSCluster.Builder(hConf).numDataNodes(1).build();
    miniDFSCluster.waitClusterUp();
    locationFactory = new FileContextLocationFactory(miniDFSCluster.getFileSystem().getConf());

    hConf = new Configuration();
    hConf.set("hadoop.security.authentication", "kerberos");
    hConf.set("hadoop.security.auth_to_local", "RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//");
    UserGroupInformation.setConfiguration(hConf);

    store = getInjector().getInstance(DefaultStore.class);
    runtimeService = getInjector().getInstance(ProgramRuntimeService.class);
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
    setKeytabDir(localKeytabDirPath.getAbsolutePath());

    // get the owner admin which has been created from the cConf which got modified above
    OwnerAdmin ownerAdmin = getOwnerAdmin();

    DefaultUGIProvider provider = new DefaultUGIProvider(cConf, locationFactory, ownerAdmin, namespaceClient, store,
                                                         runtimeService);

    // create a namespace with a principal and keytab so that later we can verify that if a required entity owner does
    // not exists then the provider gives the UGI for namespace owner
    namespaceClient.create(new NamespaceMeta.Builder().setName(namespaceId).setPrincipal(
      eveKerberosPrincipalId.getPrincipal()).setKeytabURI(eveKeytabFile.getAbsolutePath()).build());

    // add an owner for some entity
    ownerAdmin.add(aliceEntity, aliceKerberosPrincipalId);
    ownerAdmin.add(bobEntity, bobKerberosPrincipalId);

    // Try with local keytab file
    ImpersonationRequest aliceImpRequest = new ImpersonationRequest(aliceEntity, ImpersonatedOpType.OTHER);
    ImpersonationRequest bobImpRequest = new ImpersonationRequest(bobEntity, ImpersonatedOpType.OTHER);
    UGIWithPrincipal aliceUGIWithPrincipal = verifyAndGetUGI(provider, aliceKerberosPrincipalId, aliceImpRequest);
    UGIWithPrincipal bobUGIWithPrincipal = verifyAndGetUGI(provider, bobKerberosPrincipalId, bobImpRequest);

    // delete the local keytab file for bob
    Assert.assertTrue(bobKeytabFile.delete());

    // verify caching by fetch the bob UGI again, it should still return the valid one but after invalidating the
    // cache it shouldn't
    verifyCaching(provider, aliceImpRequest, bobImpRequest, aliceUGIWithPrincipal, bobUGIWithPrincipal);

    // delete the owner info for bob's entity
    ownerAdmin.delete(bobEntity);

    // now we should get the namespace owner principal if we get try to impersonate bobEntity
    UGIWithPrincipal eveUGIWithPrincipal = provider.getConfiguredUGI(bobImpRequest);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
                        eveUGIWithPrincipal.getUGI().getAuthenticationMethod());
    Assert.assertTrue(eveUGIWithPrincipal.getUGI().hasKerberosCredentials());
    Assert.assertEquals(eveKerberosPrincipalId.getPrincipal(), eveUGIWithPrincipal.getPrincipal());

    // cleanup
    ownerAdmin.delete(aliceEntity);
    ownerAdmin.delete(bobEntity);
    namespaceClient.delete(namespaceId);
  }

  @Test
  public void testDefaultUGIProviderWithHDFSFiles() throws Exception {
    // create a location on hdfs for keytabs
    Location hdfsKeytabDir = locationFactory.create("keytabs");
    // set in the cConf so that later it can be used to fetch the keytabs for the given principal
    setKeytabDir(hdfsKeytabDir.toURI().toString());

    Location aliceRemoteKeytabFile = copyFileToHDFS(hdfsKeytabDir, aliceKeytabFile);
    Location bobRemoteKeytabFile = copyFileToHDFS(hdfsKeytabDir, bobKeytabFile);

    OwnerAdmin ownerAdmin = getOwnerAdmin();
    DefaultUGIProvider provider = new DefaultUGIProvider(cConf, locationFactory, ownerAdmin, namespaceClient, store,
                                                         runtimeService);

    // add some entity owners
    ownerAdmin.add(aliceEntity, aliceKerberosPrincipalId);
    ownerAdmin.add(bobEntity, bobKerberosPrincipalId);

    // Try with keytab file on hdfs
    ImpersonationRequest aliceImpRequest = new ImpersonationRequest(aliceEntity, ImpersonatedOpType.OTHER);
    ImpersonationRequest bobImpRequest = new ImpersonationRequest(bobEntity, ImpersonatedOpType.OTHER);
    UGIWithPrincipal aliceUGIWithPrincipal = verifyAndGetUGI(provider, aliceKerberosPrincipalId, aliceImpRequest);
    UGIWithPrincipal bobUGIWithPrincipal = verifyAndGetUGI(provider, bobKerberosPrincipalId, bobImpRequest);

    // delete bob's keytab file on hdfs
    Assert.assertTrue(bobRemoteKeytabFile.delete());

    // verify caching by ensuring that we are able to fetch bob's ugi even after delete but not after invalidating the
    // cache
    verifyCaching(provider, aliceImpRequest, bobImpRequest, aliceUGIWithPrincipal, bobUGIWithPrincipal);

    // cleanup
    ownerAdmin.delete(aliceEntity);
    ownerAdmin.delete(bobEntity);
  }

  private void verifyCaching(DefaultUGIProvider provider, ImpersonationRequest aliceImpRequest,
                             ImpersonationRequest bobImpRequest, UGIWithPrincipal aliceUGIWithPrincipal,
                             UGIWithPrincipal bobUGIWithPrincipal) throws IOException {
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

  private Location copyFileToHDFS(Location hdfsKeytabDir, File localFile) throws IOException {
    Location remoteFile = hdfsKeytabDir.append(localFile.getName());
    Assert.assertTrue(remoteFile.createNew());
    Files.copy(localFile, Locations.newOutputSupplier(remoteFile));
    return remoteFile;
  }

  private UGIWithPrincipal verifyAndGetUGI(UGIProvider provider, KerberosPrincipalId principalId,
                                           ImpersonationRequest impersonationRequest) throws IOException {
    UGIWithPrincipal ugiWithPrincipal = provider.getConfiguredUGI(impersonationRequest);
    Assert.assertEquals(UserGroupInformation.AuthenticationMethod.KERBEROS,
                        ugiWithPrincipal.getUGI().getAuthenticationMethod());
    Assert.assertEquals(principalId.getPrincipal(), ugiWithPrincipal.getPrincipal());
    Assert.assertTrue(ugiWithPrincipal.getUGI().hasKerberosCredentials());

    // Fetch it again, it is should return the same UGI since there is caching
    Assert.assertSame(ugiWithPrincipal.getUGI(), provider.getConfiguredUGI(impersonationRequest).getUGI());
    return ugiWithPrincipal;
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

}
