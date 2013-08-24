/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data2.transaction.queue;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.queue.QueueName;
import com.continuuity.data2.queue.ConsumerConfig;
import com.continuuity.data2.queue.Queue2Consumer;
import com.continuuity.data2.queue.Queue2Producer;
import com.continuuity.data2.queue.QueueClientFactory;
import com.continuuity.weave.common.Threads;
import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 *
 */
public final class HBaseQueueClientFactory implements QueueClientFactory {

  // 4M write buffer for HTable
  private static final int DEFAULT_WRITE_BUFFER_SIZE = 4 * 1024 * 1024;
  private static final int MAX_EVICTION_THREAD_POOL_SIZE = 10;
  private static final int EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS = 60;

  private final HBaseAdmin admin;
  private final byte[] tableName;
  private final ExecutorService evictionExecutor;

  @Inject
  public HBaseQueueClientFactory(@Named("HBaseOVCTableHandleHConfig") Configuration hConf,
                                 @Named("HBaseOVCTableHandleCConfig") CConfiguration cConf) throws IOException {
    this(new HBaseAdmin(hConf), cConf);
  }

  public HBaseQueueClientFactory(HBaseAdmin admin, CConfiguration cConf) throws IOException {
    this.admin = admin;
    this.tableName = Bytes.toBytes(cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_NAME));
    this.evictionExecutor = createEvictionExecutor();

    String jarDir = cConf.get(QueueConstants.ConfigKeys.QUEUE_TABLE_COPROCESSOR_DIR,
                              System.getProperty("java.io.tmpdir") + "/queue");
    HBaseQueueUtils.createTableIfNotExists(admin, tableName, QueueConstants.COLUMN_FAMILY,
                                           QueueConstants.MAX_CREATE_TABLE_WAIT,
                                           createCoProcessorJar(getFileSystem(cConf, admin.getConfiguration()),
                                                                new Path(jarDir)),
                                           HBaseQueueEvictionEndpoint.class.getName());
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName) throws IOException {
    return createProducer(queueName, QueueMetrics.NOOP_QUEUE_METRICS);
  }

  @Override
  public Queue2Consumer createConsumer(QueueName queueName,
                                       ConsumerConfig consumerConfig, int numGroups) throws IOException {
    if (numGroups > 0 && consumerConfig.getInstanceId() == 0) {
      return new HBaseQueue2Consumer(consumerConfig, createHTable(), queueName,
                                     new HBaseQueueEvictor(createHTable(), queueName, evictionExecutor, numGroups));
    }
    return new HBaseQueue2Consumer(consumerConfig, createHTable(), queueName, QueueEvictor.NOOP);
  }

  @Override
  public Queue2Producer createProducer(QueueName queueName, QueueMetrics queueMetrics) throws IOException {
    return new HBaseQueue2Producer(createHTable(), queueName, queueMetrics);
  }

  private ExecutorService createEvictionExecutor() {
    return new ThreadPoolExecutor(0, MAX_EVICTION_THREAD_POOL_SIZE,
                                  EVICTION_THREAD_POOL_KEEP_ALIVE_SECONDS, TimeUnit.SECONDS,
                                  new SynchronousQueue<Runnable>(),
                                  Threads.createDaemonThreadFactory("queue-eviction-%d"),
                                  new ThreadPoolExecutor.CallerRunsPolicy());
  }

  private HTable createHTable() throws IOException {
    HTable consumerTable = new HTable(admin.getConfiguration(), tableName);
    // TODO: make configurable
    consumerTable.setWriteBufferSize(DEFAULT_WRITE_BUFFER_SIZE);
    consumerTable.setAutoFlush(false);
    return consumerTable;
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
    // Assuming the endpoint and protocol class doesn't have dependencies other than those comes with HBase and Java.
    final Set<String> acceptClasses = ImmutableSet.of(HBaseQueueEvictionEndpoint.class.getName(),
                                                      HBaseQueueEvictionProtocol.class.getName());

    File jarFile = File.createTempFile("queue", ".jar");
    try {
      final JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(jarFile));
      try {
        Dependencies.findClassDependencies(HBaseQueueEvictionEndpoint.class.getClassLoader(),
                                           new Dependencies.ClassAcceptor() {
           @Override
           public boolean accept(String className, final URL classUrl,
                                 URL classPathUrl) {
             if (acceptClasses.contains(className)) {
               try {
                 jarOutput.putNextEntry(new JarEntry(className.replace('.', File.separatorChar) + ".class"));
                 ByteStreams.copy(new InputSupplier<InputStream>() {
                   @Override
                   public InputStream getInput() throws IOException {
                     return classUrl.openStream();
                   }
                 }, jarOutput);
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
      Path targetPath = new Path(jarDir, Files.hash(jarFile, Hashing.md5()).toString() + ".jar");

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
}
