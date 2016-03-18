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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.app.Application;
import co.cask.cdap.common.lang.ClassPathResources;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authorization.Authorizer;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Tests for {@link AuthorizerClassLoader}.
 */
public class AuthorizerClassLoaderTest {
  private final ClassLoader parent = AuthorizerClassLoader.createParent();

  @Test
  public void testAuthorizerClassLoaderParentAvailableClasses() throws ClassNotFoundException {
    // classes from java.* should be available
    parent.loadClass(List.class.getName());
    // classes from javax.* should be available
    parent.loadClass(Nullable.class.getName());
    // classes from gson should be available
    parent.loadClass(Gson.class.getName());
    // classes from cdap-api should be available
    parent.loadClass(Application.class.getName());
    // classes from cdap-proto should be available
    parent.loadClass(Principal.class.getName());
    // classes from cdap-security-spi should be available
    parent.loadClass(Authorizer.class.getName());
    parent.loadClass(UnauthorizedException.class.getName());
  }

  @Test
  public void testAuthorizerClassLoaderParentUnavailableClasses() {
    // classes from guava should not be available
    assertClassUnavailable(ImmutableList.class);
    // classes from hadoop should not be available
    assertClassUnavailable(Configuration.class);
    // classes from hbase should not be available
    assertClassUnavailable(HTable.class);
    // classes from spark should not be available
    assertClassUnavailable("org.apache.spark.SparkConf");
    // classes from twill should not be available
    assertClassUnavailable(LocationFactory.class);
    // classes from logback should not be available
    assertClassUnavailable(Logger.class);
    // classes from cdap-common should not be available
    assertClassUnavailable(ClassPathResources.class);
    // classes from cdap-security should not be available
    assertClassUnavailable(AuthorizerClassLoader.class);
    // classes from cdap-data-fabric should not be available
    assertClassUnavailable("co.cask.cdap.data2.util.TableId");
    // classes from cdap-app-fabric should not be available
    assertClassUnavailable("co.cask.cdap.internal.app.namespace.DefaultNamespaceAdmin");
  }

  private void assertClassUnavailable(Class<?> aClass) {
    assertClassUnavailable(aClass.getName());
  }

  private void assertClassUnavailable(String aClassName) {
    try {
      parent.loadClass(aClassName);
      Assert.fail(String.format("Class %s should not be available from the parent class loader of the " +
                                  "AuthorizerClassLoader, but it is.", aClassName));
    } catch (ClassNotFoundException e) {
      // expected
    }
  }
}
