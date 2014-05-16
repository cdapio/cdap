package com.continuuity.security.server;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.util.ssl.KeyStoreKeyManager;
import com.unboundid.util.ssl.SSLUtil;

import java.net.InetAddress;

/**
 *
 */
public class test {

  public static void main(String[] args) throws Exception {
    InMemoryDirectoryServer ldapServer;
    SSLUtil serverSSLUtil = new SSLUtil(
      new KeyStoreKeyManager("/Users/gandu/workspace/keystore-ldap", "realtime".toCharArray()),
      null
    );

//    SSLUtil clientSSLUtil = new SSLUtil(
//      new TrustStoreTrustManager("/Users/gandu/workspace/keystore-ldap"));

    InMemoryDirectoryServerConfig config = new InMemoryDirectoryServerConfig("dc=example,dc=com");
    config.setListenerConfigs(InMemoryListenerConfig.createLDAPSConfig("LDAP", InetAddress.getByName("127.0.0.1"),
                                                                       2344, serverSSLUtil.createSSLServerSocketFactory(), null));
    Entry defaultEntry = new Entry(
      "dn: dc=example,dc=com",
      "objectClass: top",
      "objectClass: domain",
      "dc: example");
    Entry userEntry = new Entry(
      "dn: uid=user,dc=example,dc=com",
      "objectClass: inetorgperson",
      "cn: admin",
      "sn: User",
      "uid: user",
      "userPassword: realtime");
    ldapServer = new InMemoryDirectoryServer(config);
    ldapServer.addEntries(defaultEntry, userEntry);
    ldapServer.startListening();
    System.out.println("Start LDAP");
  }
}
