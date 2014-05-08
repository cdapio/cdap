package com.continuuity.data2.util.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.data2.transaction.queue.hbase.HBaseQueueAdmin;
import com.continuuity.hbase.wd.AbstractRowKeyDistributor;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
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
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.regex.Matcher;

/**
 * Common utilities for dealing with HBase.
 */
public abstract class HBaseTableUtil {
  /**
   * Represents the compression types supported for HBase tables.
   */
  public enum CompressionType {
    LZO, SNAPPY, GZIP, NONE
  }

  /**
   * Represents the bloom filter types supported for HBase tables.
   */
  public enum BloomType {
    ROW, ROWCOL, NONE
  }

  private static final Logger LOG = LoggerFactory.getLogger(HBaseTableUtil.class);

  public static final long MAX_CREATE_TABLE_WAIT = 5000L;    // Maximum wait of 5 seconds for table creation.

  // 4Mb
  public static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;

  private static final int COPY_BUFFER_SIZE = 0x1000;    // 4K
  private static final CompressionType DEFAULT_COMPRESSION_TYPE = CompressionType.SNAPPY;
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
  public void createTableIfNotExists(HBaseAdmin admin, String tableName,
                                     HTableDescriptor tableDescriptor) throws IOException {
    createTableIfNotExists(admin, Bytes.toBytes(tableName), tableDescriptor, null);
  }

  /**
   * Creates a hbase table if it does not exists. Same as calling
   * {@link #createTableIfNotExists(HBaseAdmin, byte[], HTableDescriptor, byte[][], long, TimeUnit)}
   * with timeout = {@link #MAX_CREATE_TABLE_WAIT} milliseconds.
   */
  public void createTableIfNotExists(HBaseAdmin admin, byte[] tableName,
                                     HTableDescriptor tableDescriptor,
                                     byte[][] splitKeys) throws IOException {
    createTableIfNotExists(admin, tableName, tableDescriptor, splitKeys,
                           MAX_CREATE_TABLE_WAIT, TimeUnit.MILLISECONDS);
  }

  /**
   * Create a hbase table if it does not exist. Deals with race conditions when two clients concurrently attempt to
   * create the table.
   * @param admin the hbase admin
   * @param tableName the name of the table
   * @param tableDescriptor hbase table descriptor for the new table
   * @param timeout Maximum time to wait for table creation.
   * @param timeoutUnit The TimeUnit for timeout.
   */
  public void createTableIfNotExists(HBaseAdmin admin, byte[] tableName,
                                     HTableDescriptor tableDescriptor,
                                     byte[][] splitKeys,
                                     long timeout, TimeUnit timeoutUnit) throws IOException {
    if (admin.tableExists(tableName)) {
      return;
    }
    setDefaultConfiguration(tableDescriptor, admin.getConfiguration());

    String tableNameString = Bytes.toString(tableName);

    try {
      LOG.info("Creating table '{}'", tableNameString);
      // HBaseAdmin.createTable can handle null splitKeys.
      admin.createTable(tableDescriptor, splitKeys);
      LOG.info("Table created '{}'", tableNameString);
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
      long sleepTime = timeoutUnit.toNanos(timeout) / 10;
      sleepTime = sleepTime <= 0 ? 1 : sleepTime;
      do {
        if (admin.tableExists(tableName)) {
          LOG.info("Table '{}' exists now. Assuming that another process concurrently created it.", tableName);
          return;
        } else {
          TimeUnit.NANOSECONDS.sleep(sleepTime);
        }
      } while (stopwatch.elapsedTime(timeoutUnit) < timeout);
    } catch (InterruptedException e) {
      LOG.warn("Sleeping thread interrupted.");
    }
    LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", tableName, MAX_CREATE_TABLE_WAIT);
  }


  // This is a workaround for unit-tests which should run even if compression is not supported
  // todo: this should be addressed on a general level: Reactor may use HBase cluster (or multiple at a time some of)
  //       which doesn't support certain compression type
  private void setDefaultConfiguration(HTableDescriptor tableDescriptor, Configuration conf) {
    String compression = conf.get(CFG_HBASE_TABLE_COMPRESSION, DEFAULT_COMPRESSION_TYPE.name());
    CompressionType compressionAlgo = CompressionType.valueOf(compression);
    for (HColumnDescriptor hcd : tableDescriptor.getColumnFamilies()) {
      setCompression(hcd, compressionAlgo);
      setBloomFilter(hcd, BloomType.ROW);
    }
  }

  // For simplicity we allow max 255 splits per bucket for now
  private static final int MAX_SPLIT_COUNT_PER_BUCKET = 0xff;

  public static byte[][] getSplitKeys(int splits) {
    return getSplitKeys(splits, HBaseQueueAdmin.ROW_KEY_DISTRIBUTION_BUCKETS, HBaseQueueAdmin.ROW_KEY_DISTRIBUTOR);
  }

  public static byte[][] getSplitKeys(int splits, int buckets, AbstractRowKeyDistributor keyDistributor) {
    // "1" can be used for queue tables that we know are not "hot", so we do not pre-split in this case
    if (splits == 1) {
      return new byte[0][];
    }

    byte[][] bucketSplits = keyDistributor.getAllDistributedKeys(Bytes.EMPTY_BYTE_ARRAY);
    Preconditions.checkArgument(splits >= 1 && splits <= MAX_SPLIT_COUNT_PER_BUCKET * bucketSplits.length,
                                "Number of pre-splits should be in [1.." +
                                  MAX_SPLIT_COUNT_PER_BUCKET * bucketSplits.length + "] range");


    // Splits have format: <salt bucket byte><extra byte>. We use extra byte to allow more splits than buckets:
    // salt bucket bytes are usually sequential in which case we cannot insert any value in between them.

    int splitsPerBucket = (splits + buckets - 1) / buckets;
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

  public static Location createCoProcessorJar(String filePrefix, Location jarDir,
                                              Iterable<? extends Class<? extends Coprocessor>> classes)
                                              throws IOException {
    StringBuilder buf = new StringBuilder();
    for (Class<? extends Coprocessor> c : classes) {
      buf.append(c.getName()).append(", ");
    }
    if (buf.length() == 0) {
      return null;
    }

    LOG.debug("Creating jar file for coprocessor classes: " + buf.toString());
    final Hasher hasher = Hashing.md5().newHasher();
    final byte[] buffer = new byte[COPY_BUFFER_SIZE];

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

    if (!dependentClasses.isEmpty()) {
      LOG.debug("Adding " + dependentClasses.size() + " classes to jar");
      File jarFile = File.createTempFile(filePrefix, ".jar");
      try {
        JarOutputStream jarOutput = null;
        try {
          jarOutput = new JarOutputStream(new FileOutputStream(jarFile));
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
              LOG.info("Error writing to jar", e);
              throw Throwables.propagate(e);
            }
          }
        } finally {
          if (jarOutput != null) {
            jarOutput.close();
          }
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
    // no dependent classes to add
    return null;
  }


  /**
   * Returns information for all coprocessor configured for the table.
   *
   * @return a Map from coprocessor class name to CoprocessorInfo
   */
  public static Map<String, CoprocessorInfo> getCoprocessorInfo(HTableDescriptor tableDescriptor) {
    Map<String, CoprocessorInfo> info = Maps.newHashMap();

    // Extract information about existing data janitor coprocessor
    // The following logic is copied from RegionCoprocessorHost in HBase
    for (Map.Entry<ImmutableBytesWritable, ImmutableBytesWritable> entry: tableDescriptor.getValues().entrySet()) {
      String key = Bytes.toString(entry.getKey().get()).trim();
      String spec = Bytes.toString(entry.getValue().get()).trim();

      if (!HConstants.CP_HTD_ATTR_KEY_PATTERN.matcher(key).matches()) {
        continue;
      }

      try {
        Matcher matcher = HConstants.CP_HTD_ATTR_VALUE_PATTERN.matcher(spec);
        if (!matcher.matches()) {
          continue;
        }

        String className = matcher.group(2).trim();
        Path path = matcher.group(1).trim().isEmpty() ? null : new Path(matcher.group(1).trim());
        int priority = matcher.group(3).trim().isEmpty() ? Coprocessor.PRIORITY_USER
          : Integer.valueOf(matcher.group(3));
        String cfgSpec = null;
        try {
          cfgSpec = matcher.group(4);
        } catch (IndexOutOfBoundsException ex) {
          // ignore
        }

        Map<String, String> properties = Maps.newHashMap();
        if (cfgSpec != null) {
          cfgSpec = cfgSpec.substring(cfgSpec.indexOf('|') + 1);
          // do an explicit deep copy of the passed configuration
          Matcher m = HConstants.CP_HTD_ATTR_VALUE_PARAM_PATTERN.matcher(cfgSpec);
          while (m.find()) {
            properties.put(m.group(1), m.group(2));
          }
        }
        info.put(className, new CoprocessorInfo(className, path, priority, properties));
      } catch (Exception ex) {
        LOG.warn("Coprocessor attribute '{}' has invalid coprocessor specification '{}'", key, spec, ex);
      }
    }

    return info;
  }

  public abstract void setCompression(HColumnDescriptor columnDescriptor, CompressionType type);

  public abstract void setBloomFilter(HColumnDescriptor columnDescriptor, BloomType type);

  public abstract CompressionType getCompression(HColumnDescriptor columnDescriptor);

  public abstract BloomType getBloomFilter(HColumnDescriptor columnDescriptor);

  public abstract Class<? extends Coprocessor> getTransactionDataJanitorClassForVersion();
  public abstract Class<? extends Coprocessor> getQueueRegionObserverClassForVersion();
  public abstract Class<? extends Coprocessor> getDequeueScanObserverClassForVersion();

  /**
   * Carries information about coprocessor information.
   */
  public static final class CoprocessorInfo {
    private final String className;
    private final Path path;
    private final int priority;
    private final Map<String, String> properties;

    private CoprocessorInfo(String className, Path path, int priority, Map<String, String> properties) {
      this.className = className;
      this.path = path;
      this.priority = priority;
      this.properties = ImmutableMap.copyOf(properties);
    }

    public String getClassName() {
      return className;
    }

    public Path getPath() {
      return path;
    }

    public int getPriority() {
      return priority;
    }

    public Map<String, String> getProperties() {
      return properties;
    }
  }
}
