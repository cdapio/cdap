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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.PreExecute;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;

/**
 *
 */
public class AliPreHook implements PreExecute {
  private static final Logger LOG = LoggerFactory.getLogger(AliPreHook.class);

  @Override
  public void run(SessionState sess, Set<ReadEntity> inputs, Set<WriteEntity> outputs,
                  UserGroupInformation ugi) throws Exception {

    System.out.println("Ali Pre Hook running.");
    LOG.info("Ali Pre Hook running.");
    LOG.info("Variables0: {}", sess.getHiveVariables().toString());
    LOG.info("Variables1: {}", sess.getHiveVariables());
    sess.setHiveVariables(ImmutableMap.of("a", "1"));
    sess.setHiveVariables(ImmutableMap.of("b", "1"));
    LOG.info("Variables2: {}", sess.getHiveVariables());

//    sess.add_resource(SessionState.ResourceType.JAR, "/opt/cdap/master/lib/co.cask.cdap.cdap-common-4.3.1.jar");

    String reloadableJars = sess.getConf().get(HiveConf.ConfVars.HIVERELOADABLEJARS.toString());
    sess.getConf().set(HiveConf.ConfVars.HIVERELOADABLEJARS.toString(),
                       "file:///opt/cdap/master/lib/co.cask.cdap.cdap-common-4.3.1.jar");
//    System.setProperty(HiveConf.ConfVars.HIVERELOADABLEJARS.toString(),
//                       "file:///opt/cdap/master/lib/co.cask.cdap.cdap-common-4.3.1.jar");

    LOG.info("1Methods: {}", Arrays.toString(sess.getClass().getMethods()));
//    sess.reloadAuxJars();
    Method m = sess.getClass().getMethod("loadReloadableAuxJars");
//    sess.loadReloadableAuxJars();
    m.invoke(sess);
    LOG.info("2Methods: {}", Arrays.toString(sess.getClass().getMethods()));
  }

//  @Override
  public void run2(SessionState sess, Set<ReadEntity> inputs, Set<WriteEntity> outputs,
                  UserGroupInformation ugi) throws Exception {
    String reloadableJars = sess.getConf().get(HiveConf.ConfVars.HIVERELOADABLEJARS.toString());
    sess.getConf().set(HiveConf.ConfVars.HIVERELOADABLEJARS.toString(),
                       reloadableJars + ",file:///opt/custom/lib/custom-jar-1.0.0.jar");
    sess.reloadAuxJars();
  }
}


/**



 public void org.apache.hadoop.hive.cli.CliSessionState.close(),
 public void SessionState.setConf(org.apache.hadoop.hive.conf.HiveConf),
 public void SessionState.setHiveVariables(java.util.Map),
 public org.apache.hadoop.hive.conf.HiveConf SessionState.getConf(),
 public void SessionState.setLastCommand(java.lang.String),
 public boolean SessionState.getIsVerbose(),
 public void SessionState.setCommandType(org.apache.hadoop.hive.ql.plan.HiveOperation),
 public boolean SessionState.getIsSilent(),
 public void SessionState.setIsSilent(boolean),
 public static SessionState$LogHelper SessionState.getConsole(),
 public java.util.Map SessionState.getOverriddenConfigurations(),
 public java.util.Map SessionState.getHiveVariables(),
 public java.lang.String SessionState.getCurrentDatabase(),
 public boolean SessionState.isHiveServerQuery(),
 public org.apache.hadoop.hive.ql.session.LineageState SessionState.getLineageState(),
 public java.io.File SessionState.getTmpOutputFile(),
 public void SessionState.setTmpOutputFile(java.io.File),
 public java.io.File SessionState.getTmpErrOutputFile(),
 public void SessionState.setTmpErrOutputFile(java.io.File),
 public void SessionState.deleteTmpOutputFile(),
 public void SessionState.deleteTmpErrOutputFile(),
 public void SessionState.setIsVerbose(boolean),
 public void SessionState.setIsHiveServerQuery(boolean),
 public void SessionState.setCmd(java.lang.String),
 public java.lang.String SessionState.getCmd(),
 public java.lang.String SessionState.getQueryId(),
 public java.lang.String SessionState.getSessionId(),
 public synchronized org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager
   SessionState.initTxnMgr(org.apache.hadoop.hive.conf.HiveConf) throws org.apache.hadoop.hive.ql.lockmgr.LockException,
 public org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager SessionState.getTxnMgr(),
 public org.apache.hadoop.hive.shims.HadoopShims$HdfsEncryptionShim
   SessionState.getHdfsEncryptionShim() throws org.apache.hadoop.hive.ql.metadata.HiveException,
 public static void SessionState.setCurrentSessionState(SessionState),
 public static void SessionState.detachSession(),
 public java.lang.String SessionState.getHdfsScratchDirURIString(),
 public static org.apache.hadoop.fs.Path SessionState.getLocalSessionPath(org.apache.hadoop.conf.Configuration),
 public static org.apache.hadoop.fs.Path SessionState.getHDFSSessionPath(org.apache.hadoop.conf.Configuration),
 public static org.apache.hadoop.fs.Path SessionState.getTempTableSpace(org.apache.hadoop.conf.Configuration),
 public org.apache.hadoop.fs.Path SessionState.getTempTableSpace(),
 public org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider SessionState.getAuthenticator(),
 public java.lang.Object SessionState.getActiveAuthorizer(),
 public SessionState$AuthorizationMode SessionState.getAuthorizationMode(),
 public org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider SessionState.getAuthorizer(),
 public org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer SessionState.getAuthorizerV2(),
 public java.lang.Class SessionState.getAuthorizerInterface(),
 public void SessionState.setActiveAuthorizer(java.lang.Object),
 public static org.apache.hadoop.hive.conf.HiveConf SessionState.getSessionConf(),
 public static org.apache.hadoop.hive.ql.exec.Registry SessionState.getRegistry(),
 public static org.apache.hadoop.hive.ql.exec.Registry SessionState.getRegistryForWrite(),
 public org.apache.hadoop.hive.ql.history.HiveHistory SessionState.getHiveHistory(),
 public java.lang.String SessionState.getLastCommand(),
 public static java.lang.String SessionState.getUserFromAuthenticator(),
 public void SessionState.loadAuxJars() throws java.io.IOException,
 public void SessionState.loadReloadableAuxJars() throws java.io.IOException,
 public java.lang.String SessionState.getATSDomainId(),
 public void SessionState.setATSDomainId(java.lang.String),
 public static SessionState$ResourceType SessionState.find_resource_type(java.lang.String),
 public java.lang.String SessionState.add_resource(SessionState$ResourceType,
   java.lang.String) throws java.lang.RuntimeException,
 public java.lang.String SessionState.add_resource(SessionState$ResourceType,
   java.lang.String, boolean) throws java.lang.RuntimeException,
 public java.util.List SessionState.add_resources(SessionState$ResourceType,
   java.util.Collection, boolean) throws java.lang.RuntimeException,
 public java.util.List SessionState.add_resources(SessionState$ResourceType,
   java.util.Collection) throws java.lang.RuntimeException,
 public static boolean SessionState.canDownloadResource(java.lang.String),
 public void SessionState.delete_resources(SessionState$ResourceType),
 public void SessionState.delete_resources(SessionState$ResourceType, java.util.List),
 public java.util.Set SessionState.list_resource(SessionState$ResourceType, java.util.List),
 public java.lang.String SessionState.getCommandType(),
 public org.apache.hadoop.hive.ql.plan.HiveOperation SessionState.getHiveOperation(),
 public void SessionState.setAuthorizer(org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider),
 public void SessionState.setAuthenticator(org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider),
 public org.apache.hadoop.hive.ql.session.CreateTableAutomaticGrant SessionState.getCreateTableGrants(),
 public void SessionState.setCreateTableGrants(org.apache.hadoop.hive.ql.session.CreateTableAutomaticGrant),
 public java.util.Map SessionState.getMapRedStats(),
 public void SessionState.setMapRedStats(java.util.Map),
 public void SessionState.setStackTraces(java.util.Map),
 public java.util.Map SessionState.getStackTraces(),
 public void SessionState.setOverriddenConfigurations(java.util.Map),
 public java.util.Map SessionState.getLocalMapRedErrors(),
 public void SessionState.addLocalMapRedErrors(java.lang.String, java.util.List),
 public void SessionState.setLocalMapRedErrors(java.util.Map),
 public void SessionState.setCurrentDatabase(java.lang.String),
 public boolean SessionState.isAuthorizationModeV2(),
 public static org.apache.hadoop.hive.ql.log.PerfLogger SessionState.getPerfLogger(boolean),
 public static org.apache.hadoop.hive.ql.log.PerfLogger SessionState.getPerfLogger(),
 public org.apache.hadoop.hive.ql.exec.tez.TezSessionState SessionState.getTezSession(),
 public java.lang.String SessionState.getUserName(),
 public void SessionState.setTezSession(org.apache.hadoop.hive.ql.exec.tez.TezSessionState),
 public void SessionState.applyAuthorizationPolicy() throws org.apache.hadoop.hive.ql.metadata.HiveException,
 public java.util.Map SessionState.getTempTables(),
 public java.util.Map SessionState.getTempTableColStats(),
 public java.lang.String SessionState.getUserIpAddress(),
 public void SessionState.setUserIpAddress(java.lang.String),
 public org.apache.hadoop.hive.ql.exec.spark.session.SparkSession SessionState.getSparkSession(),
 public void SessionState.setSparkSession(org.apache.hadoop.hive.ql.exec.spark.session.SparkSession),
 public java.lang.String SessionState.getNextValuesTempTableSuffix(),
 public void SessionState.setupQueryCurrentTimestamp(),
 public java.sql.Timestamp SessionState.getQueryCurrentTimestamp(),
 public void SessionState.setForwardedAddresses(java.util.List),
 public java.util.List SessionState.getForwardedAddresses(),
 public java.lang.String SessionState.getReloadableAuxJars(),
 public void SessionState.updateProgressMonitor(org.apache.hadoop.hive.common.log.ProgressMonitor),
 public org.apache.hadoop.hive.common.log.ProgressMonitor SessionState.getProgressMonitor(),
 public void SessionState.setHiveServer2Host(java.lang.String),
 public java.lang.String SessionState.getHiveServer2Host(),
 public void SessionState.setKillQuery(org.apache.hadoop.hive.ql.session.KillQuery),
 public org.apache.hadoop.hive.ql.session.KillQuery SessionState.getKillQuery(),
 public static SessionState SessionState.get(),
 public static synchronized SessionState SessionState.start(SessionState),
 public static SessionState SessionState.start(org.apache.hadoop.hive.conf.HiveConf),
 public final void java.lang.Object.wait(long, int) throws java.lang.InterruptedException,
 public final native void java.lang.Object.wait(long) throws java.lang.InterruptedException,
 public final void java.lang.Object.wait() throws java.lang.InterruptedException,
 public boolean java.lang.Object.equals(java.lang.Object),
 public java.lang.String java.lang.Object.toString(),
 public native int java.lang.Object.hashCode(),
 public final native java.lang.Class java.lang.Object.getClass(),
 public final native void java.lang.Object.notify(),
 public final native void java.lang.Object.notifyAll()




 *
 */
