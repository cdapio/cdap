/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.explore.executor;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;

/**
 *
 */
public class AliSessionHook implements HiveSessionHook {
  private static final Logger LOG = LoggerFactory.getLogger(AliSessionHook.class);

//  private static final String prefix = "file://";
  private static final String prefix = "";

  private static final String HBASE_PATH = "/usr/hdp/2.6.2.0-205/hbase/lib";
//  private static final List<String> paths =
//    ImmutableList.of("/usr/hdp/2.6.2.0-205/hbase/lib/hbase-common-1.1.2.2.6.2.0-205.jar",
//                     "/usr/hdp/2.6.2.0-205/hbase/lib/hbase-client-1.1.2.2.6.2.0-205.jar");

  @Override
  public void run(HiveSessionHookContext hiveSessionHookContext) throws HiveSQLException {
    System.out.println("Ali Session Hook running!");
    LOG.info("Ali Session Hook running!");

    PartitionKey partitionKey = PartitionKey.builder()
      .addIntField("year", 2017)
      .addIntField("month", 12)
      .addIntField("day", 6)
      .addIntField("hour", 11)
      .build();
//class org.apache.twill.internal.utils.Dependencies$DependencyClassVisitor has interface org.objectweb.asm.ClassVisitor
    URLClassLoader cl = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    LOG.info("1ClassLoader URLS({}): {}", cl.getURLs().length, cl.getURLs());

//    hiveSessionHookContext.getSessionConf().setVar(HiveConf.ConfVars.HIVEAUXJARS,
//                                                prefix + path);
//    System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(),
//                       prefix + path);

//    hiveSessionHookContext.getSessionConf().setVar(HiveConf.ConfVars.HIVERELOADABLEJARS,
//                                                   prefix + path);
//    System.setProperty(HiveConf.ConfVars.HIVERELOADABLEJARS.toString(),
//                       prefix + path);


    HiveConf hiveConf = hiveSessionHookContext.getSessionConf();


//    try {
//      File cdapSiteFile = new File("/etc/cdap/conf/cdap-site.xml");
//      Preconditions.checkState(cdapSiteFile.exists());
//      CConfiguration cConf = CConfiguration.create();
//      cConf.addResource(cdapSiteFile.toURI().toURL());
//      // TODO: how to set the cdap-site.xml into here?
////    cConf.set("local.data.dir", "/tmp");
//      ConfigurationUtil.set(hiveConf, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE, cConf);
//    } catch (IOException e) {
//      Throwables.propagate(e);
//    }


    SessionState sess = SessionState.get();

//    sess.getSessionConf().setVar(HiveConf.ConfVars.HIVEAUXJARS,
//                                                prefix + path);
//    System.setProperty(HiveConf.ConfVars.HIVEAUXJARS.toString(),
//                       prefix + path);
//
//    sess.getSessionConf().setVar(HiveConf.ConfVars.HIVERELOADABLEJARS,
//                                                   prefix + path);
//    System.setProperty(HiveConf.ConfVars.HIVERELOADABLEJARS.toString(),
//                       prefix + path);

    try {
      // this does not work for classes needed while submitting the query (only for classes needed during the execution
      // of the Hive job)

      File hbaseLibDir = new File(HBASE_PATH);
      // TODO: get the hbase classpath from cdap context instead
      for (String fileName : hbaseLibDir.list()) {
        File libFile = new File(HBASE_PATH, fileName);
        if (libFile.exists()) {
//          sess.add_resource(SessionState.ResourceType.JAR, libFile.getAbsolutePath());
        }
      }

//      for (String path : paths) {
//        sess.add_resource(SessionState.ResourceType.JAR, path);
//      }
      HiveConf conf = sess.getSessionConf();
      LOG.info("original value: {}", conf.get(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH));
      conf.set(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH,
               "/opt/cdap/master/lib/com.google.guava.guava-13.0.1.jar," +
                 "/opt/cdap/master/lib/org.ow2.asm.asm-all-5.0.3.jar," +
                 conf.get(MRJobConfig.MAPREDUCE_APPLICATION_CLASSPATH));


      LOG.info("3reloading...");
      Method m1 = sess.getClass().getMethod("loadAuxJars");
//      m1.invoke(sess);

      cl = (URLClassLoader) Thread.currentThread().getContextClassLoader();
      LOG.info("2ClassLoader URLS({}): {}", cl.getURLs().length, cl.getURLs());

      Method m2 = sess.getClass().getMethod("loadReloadableAuxJars");
//      m2.invoke(sess);

      cl = (URLClassLoader) Thread.currentThread().getContextClassLoader();
      LOG.info("3ClassLoader URLS({}): {}", cl.getURLs().length, cl.getURLs());

      cl = (URLClassLoader) hiveSessionHookContext.getSessionConf().getClassLoader();
      LOG.info("4ClassLoader URLS({}): {}", cl.getURLs().length, cl.getURLs());

      cl = (URLClassLoader) sess.getSessionConf().getClassLoader();
      LOG.info("5ClassLoader URLS({}): {}", cl.getURLs().length, cl.getURLs());


      LOG.info("3reloading!!!");
//    } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
    } catch (NoSuchMethodException e) {
      Throwables.propagate(e);
    }
//    LOG.info("Ali Session Hook running with sessionState: {}", sessionState);

  }
}
