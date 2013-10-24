package com.continuuity.data2.util.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
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
  private static final Compression.Algorithm COMPRESSION_TYPE = Compression.Algorithm.SNAPPY;
  public static final String CFG_HBASE_TABLE_COMPRESSION = "hbase.table.compression.default";

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
      setDefaultConfiguration(tableDescriptor, admin.getConfiguration());

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

  public static void createQueueTableIfNotExists(HBaseAdmin admin, byte[] tableName,
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

      setDefaultConfiguration(htd, admin.getConfiguration());

      createTableIfNotExists(admin, tableName, htd, splitKeys);
    }
  }

  // This is a workaround for unit-tests which should run even if compression is not supported
  // todo: this should be addressed on a general level: Reactor may use HBase cluster (or multiple at a time some of)
  //       which doesn't support certain compression type
  private static void setDefaultConfiguration(HTableDescriptor tableDescriptor, Configuration conf) {
    String compression = conf.get(CFG_HBASE_TABLE_COMPRESSION, COMPRESSION_TYPE.name());
    Compression.Algorithm compressionAlgo = Compression.Algorithm.valueOf(compression);
    for (HColumnDescriptor hcd : tableDescriptor.getColumnFamilies()) {
      hcd.setCompressionType(compressionAlgo);
      hcd.setBloomFilterType(StoreFile.BloomType.ROW);
    }
  }

  // For simplicity we allow max 255 splits per bucket for now
  private static final int MAX_SPLIT_COUNT_PER_BUCKET = 0xff;

  static byte[][] getSplitKeys(int splits) {
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    if (splits == 1) {
      return new byte[0][];
    }

    byte[][] bucketSplits = HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR.getAllDistributedKeys(Bytes.EMPTY_BYTE_ARRAY);
    Preconditions.checkArgument(splits >= 1 && splits <= MAX_SPLIT_COUNT_PER_BUCKET * bucketSplits.length,
                                "Number of pre-splits should be in [1.." +
                                  MAX_SPLIT_COUNT_PER_BUCKET * bucketSplits.length + "] range");


    // Splits have format: <salt bucket byte><extra byte>. We use extra byte to allow more splits than buckets:
    // salt bucket bytes are usually sequential in which case we cannot insert any value in between them.

    int splitsPerBucket =
      (splits + HBaseQueueAdmin.ROW_KEY_DISTRIBUTION_BUCKETS - 1) / HBaseQueueAdmin.ROW_KEY_DISTRIBUTION_BUCKETS;
    splitsPerBucket = splitsPerBucket == 0 ? 1 : splitsPerBucket;

    byte[][] splitKeys = new byte[bucketSplits.length * splitsPerBucket - 1][];

    int prefixesPerSplitInBucket = (MAX_SPLIT_COUNT_PER_BUCKET + 1) / splitsPerBucket;

    for (int i = 0; i < bucketSplits.length; i++) {
      for (int k = 0; k < splitsPerBucket; k++) {
        if (i == 0 && k == 0) {
          // hbase will figure out first split
          continue;
        }
        int splitStartPrefix = k * prefixesPerSplitInBucket;
        int thisSplit = i * splitsPerBucket + k - 1;
        if (splitsPerBucket > 1) {
          splitKeys[thisSplit] = new byte[] {(byte) i, (byte) splitStartPrefix};
        } else {
          splitKeys[thisSplit] = new byte[] {(byte) i};
        }
      }
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
      if (!jarDir.mkdirs() && !jarDir.exists()) {
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
