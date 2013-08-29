package com.continuuity.data2.transaction.queue.hbase;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.data2.dataset.lib.table.hbase.HBaseTableUtil;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.data2.transaction.queue.QueueConstants;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Throwables;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

  @Inject
  public HBaseQueueAdmin(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                         @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf) throws IOException {
    this.admin = new HBaseAdmin(hConf);
    this.cConf = cConf;
  }

  @Override
  public boolean exists(String name) throws Exception {
    return admin.tableExists(name);
  }

  @Override
  public void create(String name) throws Exception {
    byte[] tableName = Bytes.toBytes(name);
    String jarDir = cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                              System.getProperty("java.io.tmpdir") + "/queue");
    HBaseQueueUtils.createTableIfNotExists(admin, tableName, QueueConstants.COLUMN_FAMILY,
                                           QueueConstants.MAX_CREATE_TABLE_WAIT,
                                           createCoProcessorJar(getFileSystem(cConf, admin.getConfiguration()),
                                                                new Path(jarDir)),
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

  private FileSystem getFileSystem(CConfiguration cConfig, Configuration hConfig) throws IOException {
    String hdfsUser = cConfig.get(Constants.CFG_HDFS_USER);
    if (hdfsUser == null) {
      return FileSystem.get(FileSystem.getDefaultUri(hConfig), hConfig);
    } else {
      try {
        return FileSystem.get(FileSystem.getDefaultUri(hConfig), hConfig, hdfsUser);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }

  /**
   * Creates a jar files container coprocessors that are using by queue.
   * @param fileSystem
   * @param jarDir
   * @return The Path of the jar file on the file system.
   * @throws IOException
   */
  private Path createCoProcessorJar(FileSystem fileSystem, Path jarDir) throws IOException {
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
      Path targetPath = new Path(jarDir, "coprocessor" + hasher.hash().toString() + ".jar");

      // If the file exists and having same since, assume the file doesn't changed
      if (fileSystem.exists(targetPath) && fileSystem.getFileStatus(targetPath).getLen() == jarFile.length()) {
        return targetPath;
      }

      // Copy jar file into filesystem
      if (!fileSystem.mkdirs(jarDir)) {
        System.out.println("Fail to create");
      }
      fileSystem.copyFromLocalFile(false, true, new Path(jarFile.toURI()), targetPath);
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
