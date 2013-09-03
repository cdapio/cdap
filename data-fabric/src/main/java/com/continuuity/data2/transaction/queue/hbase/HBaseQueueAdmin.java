package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseTableUtil;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.weave.filesystem.Location;
import com.continuuity.weave.filesystem.LocationFactory;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Throwables;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 *
 */
@Singleton
public class HBaseQueueAdmin implements QueueAdmin {
  private static final int COPY_BUFFER_SIZE = 0x1000;    // 4K

  private final HBaseAdmin admin;
  private final CConfiguration cConf;
  private final LocationFactory locationFactory;

  @Inject
  public HBaseQueueAdmin(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                         @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf,
                         LocationFactory locationFactory) throws IOException {
    this.admin = new HBaseAdmin(hConf);
    this.cConf = cConf;
    this.locationFactory = locationFactory;
  }

  @Override
  public boolean exists(String name) throws Exception {
    return admin.tableExists(name);
  }

  @Override
  public void create(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(name);
    Location jarDir = locationFactory.create(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                                                       "/queue"));
    int splits = cConf.getInt(QueueConstants.ConfigKeys.QUEUE_TABLE_PRESPLITS,
                              QueueConstants.DEFAULT_QUEUE_TABLE_PRESPLITS);
    HBaseQueueUtils.createTableIfNotExists(admin, tableName, QueueConstants.COLUMN_FAMILY,
                                           QueueConstants.MAX_CREATE_TABLE_WAIT,
                                           //  For demo purposes in 1.7 we do not do eviction with CPs
                                           //  TODO: this should not go into production
                                           //  createCoProcessorJar(jarDir),
                                           null,
                                           splits,
                                           HBaseQueueEvictionEndpoint.class.getName());
  }

  @Override
  public void truncate(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(name);
    HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    admin.createTable(tableDescriptor);
  }

  @Override
  public void drop(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(name);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
  }

  /**
   * Creates a jar files container coprocessors that are using by queue.
   * @param jarDir
   * @return The Path of the jar file on the file system.
   * @throws IOException
   */
  private Location createCoProcessorJar(Location jarDir) throws IOException {
    final Hasher hasher = Hashing.md5().newHasher();
    final byte[] buffer = new byte[COPY_BUFFER_SIZE];
    File jarFile = File.createTempFile("queue", ".jar");
    try {
      final JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile));
      try {
        Dependencies.findClassDependencies(
          HBaseQueueEvictionEndpoint.class.getClassLoader(),
          new Dependencies.ClassAcceptor() {
             @Override
             public boolean accept(String className, final URL classUrl, URL classPathUrl) {
               // Assuming the endpoint and protocol class doesn't have dependencies
               // other than those comes with HBase and Java.
               if (className.startsWith("com.continuuity")) {
                 try {
                   jarOutput.putNextEntry(new JarEntry(className.replace('.', File.separatorChar) + ".class"));
                   InputStream inputStream = classUrl.openStream();

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
                   return true;
                 } catch (IOException e) {
                   throw Throwables.propagate(e);
                 }
               }
               return false;
             }
          }, HBaseQueueEvictionEndpoint.class.getName());
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

  @Override
  public void dropAll() throws Exception {
    // hack: we know that all queues are stored in one table
    String tableName = HBaseTableUtil.getHBaseTableName(cConf, cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_NAME));
    drop(tableName);
  }
}
