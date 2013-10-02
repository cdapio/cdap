package com.continuuity.data2.util.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseOcTableManager;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 * Common utilities for dealing with HBase.
 */
public class HBaseTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  // 4Mb
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  public static final String PROPERTY_TTL = "ttl";
  private static final int COPY_BUFFER_SIZE = 0x1000;    // 4K

  public static String getHBaseTableName(String tableName) {
    return encodeTableName(tableName);
  }

  private static String encodeTableName(String tableName) {
    try {
      return URLEncoder.encode(tableName, "ASCII");
    } catch (UnsupportedEncodingException e) {
      // this can never happen - we know that ASCII is a supported character set!
      LOG.error("Error encoding table name '" + tableName + "'", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Create a hbase table if it does not exist. Deals with race conditions when two clients concurrently attempt to
   * create the table.
   * @param admin the hbase admin
   * @param tableName the name of the table
   * @param tableDescriptor hbase table descriptor for the new table
   */
  public static void createTableIfNotExists(HBaseAdmin admin, String tableName,
                                            HTableDescriptor tableDescriptor) throws IOException {
    createTableIfNotExists(admin, Bytes.toBytes(tableName), tableDescriptor, null);
  }

  /**
   * Create a hbase table if it does not exist. Deals with race conditions when two clients concurrently attempt to
   * create the table.
   * @param admin the hbase admin
   * @param tableName the name of the table
   * @param tableDescriptor hbase table descriptor for the new table
   */
  public static void createTableIfNotExists(HBaseAdmin admin, byte[] tableName,
    HTableDescriptor tableDescriptor, byte[][] splitKeys) throws IOException {
    if (!admin.tableExists(tableName)) {
      String tableNameString = Bytes.toString(tableName);

      try {
        LOG.info("Creating table '{}'", tableNameString);
        if (splitKeys != null) {
          admin.createTable(tableDescriptor, splitKeys);
        } else {
          admin.createTable(tableDescriptor);
        }
        return;
      } catch (TableExistsException e) {
        // table may exist because someone else is creating it at the same
        // time. But it may not be available yet, and opening it might fail.
        LOG.info("Failed to create table '{}'. {}.", tableNameString, e.getMessage(), e);
      }

      // Wait for table to materialize
      try {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        while (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < MAX_CREATE_TABLE_WAIT) {
          if (admin.tableExists(tableName)) {
            LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.", tableName);
            return;
          } else {
            TimeUnit.MILLISECONDS.sleep(100);
          }
        }
      } catch (InterruptedException e) {
        LOG.warn("Sleeping thread interrupted.");
      }
      LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", tableName, MAX_CREATE_TABLE_WAIT);
    }
  }

  /**
   * Creates a HBase queue table if the table doesn't exists.
   *
   * @param admin
   * @param tableName
   * @param maxWaitMs
   * @param coProcessorJar
   * @param coProcessors
   * @throws IOException
   */

  public static void createTableIfNotExists(HBaseAdmin admin, byte[] tableName,
                                            byte[] columnFamily, long maxWaitMs,
                                            int splits, Location coProcessorJar,
                                            String... coProcessors) throws IOException {
    // todo consolidate this method with HBaseTableUtil
    if (!admin.tableExists(tableName)) {
      HTableDescriptor htd = new HTableDescriptor(tableName);
      if (coProcessorJar != null) {
        for (String coProcessor : coProcessors) {
          htd.addCoprocessor(coProcessor, new Path(coProcessorJar.toURI()), Coprocessor.PRIORITY_USER, null);
        }
      }

      HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
      htd.addFamily(hcd);
      hcd.setMaxVersions(1);

      byte[][] splitKeys = getSplitKeys(splits);

      createTableIfNotExists(admin, tableName, htd, splitKeys);
    }
  }

  // For simplicity we allow max 255 splits for now
  private static final int MAX_SPLIT_COUNT = 0xff;

  static byte[][] getSplitKeys(int splits) {
    Preconditions.checkArgument(splits >= 1 && splits <= MAX_SPLIT_COUNT,
                                "Number of pre-splits should be in [1.." + MAX_SPLIT_COUNT + "] interval");

    if (splits == 1) {
      return new byte[0][];
    }

    int prefixesPerSplit = (MAX_SPLIT_COUNT + 1) / splits;

    // HBase will figure out first split to be started from beginning
    byte[][] splitKeys = new byte[splits - 1][];
    for (int i = 0; i < splits - 1; i++) {
      // "1 + ..." to make it a bit more fair
      int splitStartPrefix = (i + 1) * prefixesPerSplit;
      splitKeys[i] = new byte[] {(byte) splitStartPrefix};
    }

    return splitKeys;
  }

  /**
   * Creates a jar files container coprocessors that are using by queue.
   * @param jarDir
   * @return The Path of the jar file on the file system.
   * @throws java.io.IOException
   */
  public static Location createCoProcessorJar(String filePrefix, Location jarDir,
                                              Class<? extends Coprocessor>... classes) throws IOException {
    final Hasher hasher = Hashing.md5().newHasher();
    final byte[] buffer = new byte[COPY_BUFFER_SIZE];
    File jarFile = File.createTempFile(filePrefix, ".jar");
    try {
      final JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile));
      try {
        final Map<String, URL> dependentClasses = new HashMap<String, URL>();
        for (Class<? extends Coprocessor> clz : classes) {
          Dependencies.findClassDependencies(clz.getClassLoader(), new Dependencies.ClassAcceptor() {
            @Override
            public boolean accept(String className, final URL classUrl, URL classPathUrl) {
              // Assuming the endpoint and protocol class doesn't have dependencies
              // other than those comes with HBase and Java.
              if (className.startsWith("com.continuuity")) {
                if (!dependentClasses.containsKey(className)) {
                  dependentClasses.put(className, classUrl);
                }
                return true;
              }
              return false;
            }
          }, clz.getName());
        }
        for (Map.Entry<String, URL> entry : dependentClasses.entrySet()) {
          try {
            jarOutput.putNextEntry(new JarEntry(entry.getKey().replace('.', File.separatorChar) + ".class"));
            InputStream inputStream = entry.getValue().openStream();

            try {
              int len = inputStream.read(buffer);
              while (len >= 0) {
                hasher.putBytes(buffer, 0, len);
                jarOutput.write(buffer, 0, len);
                len = inputStream.read(buffer);
              }
            } finally {
              inputStream.close();
            }
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      } finally {
        jarOutput.close();
      }
      // Copy jar file into HDFS
      // Target path is the jarDir + jarMD5.jar
      final Location targetPath = jarDir.append("coprocessor" + hasher.hash().toString() + ".jar");

      // If the file exists and having same since, assume the file doesn't changed
      if (targetPath.exists() && targetPath.length() == jarFile.length()) {
        return targetPath;
      }

      // Copy jar file into filesystem
      if (!jarDir.mkdirs()) {
        throw new IOException("Fails to create directory: " + jarDir.toURI());
      }
      Files.copy(jarFile, new OutputSupplier<OutputStream>() {
        @Override
        public OutputStream getOutput() throws IOException {
          return targetPath.getOutputStream();
        }
      });
      return targetPath;

    } finally {
      jarFile.delete();
    }
  }
}
