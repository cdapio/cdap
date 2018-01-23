/*
 * Copyright © 2017 Cask Data, Inc.
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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.CConfigurationUtil;
import co.cask.cdap.common.conf.ConfigurationUtil;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.module.lib.DatasetModules;
import co.cask.cdap.data2.dataset2.module.lib.hbase.HBaseTableModule;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import co.cask.cdap.hive.context.CConfCodec;
import co.cask.cdap.hive.stream.StreamStorageHandler;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.tephra.Transaction;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.utils.Dependencies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;
import javax.annotation.Nullable;

/**
 *
 */
public class HiveJdbcClient {
  private static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

  private static String hostname;
  static {
    try {
      hostname = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private static final String BASE_URL =
    // must be run from the same node where hiveserver2 is running
    String.format("jdbc:hive2://%s:10000/default;principal=hive/_HOST@CONTINUUITY.NET", hostname);

  // tableTypes:
  private static final String STREAM = "s";
  private static final String KV_TABLE = "k";
  private static final String PFS = "p";

  private static void print(ResultSet resultSet, String tableType) throws SQLException {
    switch (tableType) {
      case STREAM:
        System.out.println(resultSet.getLong(1) + "\t" + resultSet.getString(2) + "\t" + resultSet.getString(3));
        return;
      case KV_TABLE:
        System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
        return;
      case PFS:
        System.out.println(resultSet.getString(1) + "\t" + resultSet.getLong(2) + "\t" + resultSet.getInt(3));
        return;
      default:
        throw new IllegalArgumentException();
    }
  }

  public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, URISyntaxException {
    Class.forName(HIVE_JDBC_DRIVER_NAME);
    t1("stream_ali", STREAM);
//    t1("dataset_foo", KV_TABLE);
//    t2();
  }

  private static Connection getConnection(Map<String, String> args) throws SQLException {
    Properties props = new Properties();
    props.putAll(args);
    props.put("user", "hive"); // don't need this
    return DriverManager.getConnection(BASE_URL, props);
  }

  private static Connection getConnection2(Map<String, String> args) throws SQLException, UnsupportedEncodingException {
    StringBuilder sb = new StringBuilder("?");

    for (Entry<String, String> entry : args.entrySet()) {
      sb.append(entry.getKey()).append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8")).append(";");
    }

    return DriverManager.getConnection(BASE_URL + sb.toString(), "hive", "");
  }

  private static void t1(String tableName, String tableType) throws SQLException, IOException,
    ClassNotFoundException, URISyntaxException {
    Map<String, String> props = new HashMap<>();
    props.put("explore.hive.query.tx.id",
              new Gson().toJson(new Transaction(1000L, 1000L, new long[0], new long[0], 100L)));
    props.put("mapreduce.job.user.classpath.first", "true");
//    props.put("hbase.zookeeper.quorum", hostname);



    // TODO: cdap-site.xml is now in the classpath...
    Map<String, String> additionalArgs = new HashMap<>();
    File cdapSiteFile = new File("/etc/cdap/conf/cdap-site.xml");
    Preconditions.checkState(cdapSiteFile.exists());
    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(cdapSiteFile.toURI().toURL());
    ConfigurationUtil.set(additionalArgs, Constants.Explore.CCONF_KEY, CConfCodec.INSTANCE, cConf);

    LOG.info("root namespace: {}", cConf.get("root.namespace"));


    for (Entry<String, String> stringStringEntry : additionalArgs.entrySet()) {
      LOG.info("{}::{}", stringStringEntry.getKey(), stringStringEntry.getValue());
    }

    LOG.info("additionalArgs3: {}", additionalArgs);


//    Connection conn = getConnection(additionalArgs);
    Connection conn = getConnection2(additionalArgs);
    Statement statement = conn.createStatement();


//    addFatJar(statement);

//    String query = "show tables \'" + tableName + "\'";
    String query = "show tables";
    System.out.println("Running: " + query);
    ResultSet resultSet = statement.executeQuery(query);
    while (resultSet.next()) {
      System.out.println(resultSet.getString(1));
    }



    String path = new File("/tmp/cdap-explore2-4.3.1.jar").getAbsolutePath();
    query = "add jar " + path;
    System.out.println("Running: " + query);
    statement.execute(query); // will always return false for this statement



    query = "list jars";
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);
    while (resultSet.next()) {
      System.out.println(resultSet.getString(1));
    }




    query = "describe " + tableName;
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);

    while (resultSet.next()) {
      System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
    }

    query = "select * from " + tableName;
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);

    for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
      System.out.println("col #" + i + ": " + resultSet.getMetaData().getColumnName(i) + "(" +
                           resultSet.getMetaData().getColumnTypeName(i) + ")");
    }

    while (resultSet.next()) {
      print(resultSet, tableType);
    }

    query = "select count(1) from " + tableName + " LIMIT 1";
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);

    while (resultSet.next()) {
      System.out.println(resultSet.getString(1));
    }

  }

  private static void addFatJar(Statement statement) throws URISyntaxException, IOException, SQLException {
    // TODO: write hbase conf and cdap conf to a file, tell hive to take THAT.
    // dont include hadoop classpath/conf to this list

//    Location location = ensureCoprocessorExists(true, cConf, HBaseConfiguration.create(), dependencyJars);

//    String classPathString = System.getProperty("java.class.path");
//    String[] dependencies = classPathString.split(":");

    List<URL> dependencyJars = new ArrayList<>();

    URLClassLoader urlClassLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    List<URL> classLoaderURLs = new ArrayList<>(Arrays.asList(urlClassLoader.getURLs()));
    System.out.println("classLoaderURLs: " + classLoaderURLs);

    ImmutableSet<String> disallowedPaths = ImmutableSet.of("/home/ali", "/etc/cdap/conf", "/etc/hadoop");
    outerFor:
    for (URL classLoaderURL : classLoaderURLs) {
      if (!new File(classLoaderURL.toURI()).exists()) {
        continue;
      }
      for (String disallowPath : disallowedPaths) {
        if (classLoaderURL.getPath().contains(disallowPath)) {
          continue outerFor;
        }
      }
      if (isHadoopClassPath(classLoaderURL)) {
        System.out.println("hadoopURL being skipped: " + classLoaderURL);
        continue;
      }
      if (isConflictingJar(classLoaderURL)) {
        System.out.println("Conflicting Jar: " + classLoaderURL);
        continue;
      }
      dependencyJars.add(classLoaderURL);
    }

//    String query = "add jar /tmp/fat.jar";
//    String query = "add jar " + location.toURI().getPath();
//    String query = "add jar " + Joiner.on(" ").join(dependencyJars);
    String query = "add jar " + Joiner.on(" ").join(dependencyJars);
    System.out.println("Running: " + query);
    statement.execute(query); // will always return false for this statement
  }


  private static final Set<String> conflictingPaths =
    ImmutableSet.of("/usr/hdp/2.6.2.0-205/hive/lib/guava-14.0.1.jar",
                    "/usr/hdp/2.6.2.0-205/hbase/lib/guava-12.0.1.jar",
                    "/usr/hdp/2.6.2.0-205/hive/lib/asm-commons-3.1.jar",
                    "/usr/hdp/2.6.2.0-205/hive/lib/asm-tree-3.1.jar",
                    "/usr/hdp/2.6.2.0-205/hbase/lib/asm-3.1.jar");

  private static boolean isConflictingJar(URL url) {
    return conflictingPaths.contains(url.getPath());
  }

  private static boolean isHadoopClassPath(URL uri) throws IOException {
    Path path = Paths.get(uri.getPath()).toRealPath();
    for (Path hadoopFile : getHadoopClasspathFiles()) {
      if (path.startsWith(hadoopFile)) {
        return true;
      }
    }
    return false;
  }

  private static List<Path> hadoopClassPath;

  private static List<Path> getHadoopClasspathFiles() {
    if (hadoopClassPath != null) {
      return hadoopClassPath;
    }
    String hadoopClasspath =
      "/usr/hdp/2.6.2.0-205/hadoop/conf:/usr/hdp/2.6.2.0-205/hadoop/lib/*:/usr/hdp/2.6.2.0-205/hadoop/.//*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-hdfs/./:/usr/hdp/2.6.2.0-205/hadoop-hdfs/lib/*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-hdfs/.//*:/usr/hdp/2.6.2.0-205/hadoop-yarn/lib/*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-yarn/.//*:/usr/hdp/2.6.2.0-205/hadoop-mapreduce/lib/*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-mapreduce/.//*::/etc/tez/conf.chef:/usr/hdp/current/tez-client/*:" +
        "/usr/hdp/current/tez-client/lib/*:/usr/hdp/2.6.2.0-205/tez/*:/usr/hdp/2.6.2.0-205/tez/lib/*:" +
        "/usr/hdp/2.6.2.0-205/tez/conf";


    hadoopClasspath =
      "/usr/hdp/2.6.2.0-205/hadoop/conf:/usr/hdp/2.6.2.0-205/hadoop/lib/*:/usr/hdp/2.6.2.0-205/hadoop/.//*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-hdfs/./:/usr/hdp/2.6.2.0-205/hadoop-hdfs/lib/*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-hdfs/.//*:/usr/hdp/2.6.2.0-205/hadoop-yarn/lib/*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-yarn/.//*:/usr/hdp/2.6.2.0-205/hadoop-mapreduce/lib/*:" +
        "/usr/hdp/2.6.2.0-205/hadoop-mapreduce/.//*:/usr/hdp/current/tez-client/*:" +
        "/usr/hdp/current/tez-client/lib/*:/usr/hdp/2.6.2.0-205/tez/*:/usr/hdp/2.6.2.0-205/tez/lib/*:" +
        "/usr/hdp/2.6.2.0-205/tez/conf";
    List<String> hadoopClasspathFiles = Arrays.asList(hadoopClasspath.split(":"));

    // TODO: filter ':'
    // TODO: filter files that don't exist

    return hadoopClassPath = Lists.transform(hadoopClasspathFiles, new Function<String, Path>() {
      @Nullable
      @Override
      public Path apply(String input) {
        Preconditions.checkArgument(!input.isEmpty());
        if ('*' == input.charAt(input.length() - 1)) {
          input =  input.substring(0, input.length() - 1);
        }
        try {
          return Paths.get(input).toRealPath();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  private static void t2() throws SQLException {
    Connection conn = DriverManager.getConnection(BASE_URL, "hive", "");
    Statement statement = conn.createStatement();
    String tableName = "testHiveDriverTable";
    statement.execute("drop table if exists " + tableName);
    statement.execute("create table " + tableName + " (key int, value string)");
    String query = "show tables \'" + tableName + "\'";
    System.out.println("Running: " + query);
    ResultSet resultSet = statement.executeQuery(query);
    if (resultSet.next()) {
      System.out.println(resultSet.getString(1));
    }


    query = "describe " + tableName;
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);

    while (resultSet.next()) {
      System.out.println(resultSet.getString(1) + "\t" + resultSet.getString(2));
    }

    String dataPath = "/tmp/a.txt";
    query = "load data local inpath \'" + dataPath + "\' into table " + tableName;
    System.out.println("Running: " + query);
    statement.execute(query);
    query = "select * from " + tableName;
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);

    while (resultSet.next()) {
      System.out.println(resultSet.getInt(1) + "\t" + resultSet.getString(2));
    }

    query = "select count(1) from " + tableName;
    System.out.println("Running: " + query);
    resultSet = statement.executeQuery(query);

    while (resultSet.next()) {
      System.out.println(resultSet.getString(1));
    }

  }


  private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcClient.class);

  public static synchronized Location ensureCoprocessorExists(boolean overwrite,
                                                              CConfiguration cConf,
                                                              Configuration hBaseConfiguration,
                                                              final Set<String> dependencyJars)
    throws IOException, ClassNotFoundException {

    File tmpDir = Files.createTempDir();

    LocalLocationFactory localLocationFactory = new LocalLocationFactory(tmpDir);

    final Location jarDependencyFile =
      localLocationFactory.create(String.format("cdap-dependencies-%s.jar", System.currentTimeMillis()));
    if (!overwrite && jarDependencyFile.exists()) {
      return jarDependencyFile;
    }

    // ensure the jar directory exists
//    Locations.mkdirsIfNotExists(jarDir); // not needed because of Files.createTempDir() above


    System.out.println("Creating jar file: " + jarDependencyFile);

//    HBaseTableUtilFactory.getHBaseTableUtilClass()


    final Map<String, URL> dependentClasses = new HashMap<>();
    final Set<String> addedJars = new HashSet<>();
    for (Class<?> clz : ImmutableSet.of(StreamStorageHandler.class
                                        , HBaseTableUtilFactory.getHBaseTableUtilClass(cConf)
                                        , HBaseTableModule.class
                                        , DatasetModules.class
    )) {
      Dependencies.findClassDependencies(clz.getClassLoader(), new ClassAcceptor() {
        @Override
        public boolean accept(String className, final URL classUrl, URL classPathUrl) {
          // Assuming the endpoint and protocol class doesn't have dependencies
          // other than those comes with HBase, Java, fastutil, and gson
//          if (className.startsWith("co.cask") || className.startsWith("it.unimi.dsi.fastutil")
//            || className.startsWith("org.apache.tephra") || className.startsWith("com.google.gson")) {
          if (!dependentClasses.containsKey(className)) {
            dependentClasses.put(className, classUrl);

            String path = classUrl.getPath();
            int idx = path.indexOf("!");
            if (idx != -1) {
              path = path.substring(0, idx);
            }
            dependencyJars.add(path);
//            addedJars.add(path);
          }
          return true;
//          }
//          return false;
        }
      }, clz.getName());
    }

    System.out.println(dependencyJars);

    if (dependentClasses.isEmpty()) {
      return null;
    }

    // create the coprocessor jar on local filesystem
    LOG.debug("Adding " + dependentClasses.size() + " classes to jar");
    File jarFile = File.createTempFile("coprocessor", ".jar");
    byte[] buffer = new byte[4 * 1024];
    try (JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile))) {


      jarOutput.putNextEntry(new ZipEntry("cdap-site.xml"));
      cConf.writeXml(jarOutput);

      jarOutput.putNextEntry(new ZipEntry("hbase-site.xml"));
      hBaseConfiguration.writeXml(jarOutput);



      for (Map.Entry<String, URL> entry : dependentClasses.entrySet()) {
        jarOutput.putNextEntry(new JarEntry(entry.getKey().replace('.', File.separatorChar) + ".class"));

        try (InputStream inputStream = entry.getValue().openStream()) {
          int len = inputStream.read(buffer);
          while (len >= 0) {
            jarOutput.write(buffer, 0, len);
            len = inputStream.read(buffer);
          }
        }
      }
    } catch (IOException e) {
      LOG.error("Unable to create temporary local coprocessor jar {}.", jarFile.getAbsolutePath(), e);
      if (!jarFile.delete()) {
        LOG.warn("Unable to clean up temporary local coprocessor jar {}.", jarFile.getAbsolutePath());
      }
      throw e;
    }

    // copy the local jar file to the filesystem (HDFS)
    // copies to a tmp location then renames the tmp location to the target location in case
    // multiple CoprocessorManagers we called at the same time. This should never be the case in distributed
    // mode, as coprocessors should all be loaded beforehand using the CoprocessorBuildTool.
    final Location tmpLocation = jarDependencyFile.getTempFile(".jar");
    try {
      // Copy jar file into filesystem (HDFS)
      Files.copy(jarFile, new OutputSupplier<OutputStream>() {
        @Override
        public OutputStream getOutput() throws IOException {
          return tmpLocation.getOutputStream();
        }
      });
    } catch (IOException e) {
      LOG.error("Unable to copy local coprocessor jar to filesystem at {}.", tmpLocation, e);
      if (tmpLocation.exists()) {
        LOG.info("Deleting partially copied coprocessor jar at {}.", tmpLocation);
        try {
          if (!tmpLocation.delete()) {
            LOG.error("Unable to delete partially copied coprocessor jar at {}.", tmpLocation, e);
          }
        } catch (IOException e1) {
          LOG.error("Unable to delete partially copied coprocessor jar at {}.", tmpLocation, e1);
          e.addSuppressed(e1);
        }
      }
      throw e;
    } finally {
      if (!jarFile.delete()) {
        LOG.warn("Unable to clean up temporary local coprocessor jar {}.", jarFile.getAbsolutePath());
      }
    }

    tmpLocation.renameTo(jarDependencyFile);
    return jarDependencyFile;
  }

}

