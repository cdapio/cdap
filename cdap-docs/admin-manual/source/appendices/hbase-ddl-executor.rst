.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2017 Cask Data, Inc.

.. _appendix-hbase-ddl-executor:

==========================
Appendix: HBaseDDLExecutor
==========================

For details on replication of CDAP clusters, see :ref:`CDAP Replication <installation-replication>`.

This document provides a sample implementation of ``HBaseDDLExecutor``.

The ``HBaseDDLExecutorImpl`` class performs the DDL operations on the local and peer cluster
(master and slave).

Interface
=========
Interface of ``HBaseDDLExecutor``. Note that this is a *Beta* feature and subject to change without notice.

.. literalinclude:: /../../../cdap-hbase-spi/src/main/java/co/cask/cdap/spi/hbase/HBaseDDLExecutor.java
   :language: java
   :lines: 27-


Implementation
==============
Sample implementation of ``HBaseDDLExecutor``, for HBase version 1.0.0-cdh5.5.1::

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
  package com.example.hbase.ddlexecutor;
 
  import co.cask.cdap.spi.hbase.ColumnFamilyDescriptor;
  import co.cask.cdap.spi.hbase.CoprocessorDescriptor;
  import co.cask.cdap.spi.hbase.HBaseDDLExecutor;
  import co.cask.cdap.spi.hbase.HBaseDDLExecutorContext;
  import co.cask.cdap.spi.hbase.TableDescriptor;
  import com.google.common.base.Preconditions;
  import com.google.common.base.Stopwatch;
  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.hbase.HColumnDescriptor;
  import org.apache.hadoop.hbase.HConstants;
  import org.apache.hadoop.hbase.HTableDescriptor;
  import org.apache.hadoop.hbase.NamespaceDescriptor;
  import org.apache.hadoop.hbase.NamespaceNotFoundException;
  import org.apache.hadoop.hbase.TableExistsException;
  import org.apache.hadoop.hbase.TableName;
  import org.apache.hadoop.hbase.TableNotDisabledException;
  import org.apache.hadoop.hbase.TableNotEnabledException;
  import org.apache.hadoop.hbase.client.HBaseAdmin;
  import org.apache.hadoop.hbase.io.compress.Compression;
  import org.apache.hadoop.hbase.util.Bytes;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;
 
  import java.io.IOException;
  import java.io.StringWriter;
  import java.io.UnsupportedEncodingException;
  import java.net.URLEncoder;
  import java.util.Map;
  import java.util.concurrent.TimeUnit;
 
  /**
   * Sample implementation of {@link HBaseDDLExecutor} for HBase version 1.0.0-cdh5.5.1
   */
  public class HBaseDDLExecutorImpl implements HBaseDDLExecutor {
    public static final Logger LOG = LoggerFactory.getLogger(HBaseDDLExecutorImpl.class);
    private HBaseAdmin admin;
    private HBaseAdmin peerAdmin;
 
    /**
     * Encode a HBase entity name to ASCII encoding using {@link URLEncoder}.
     *
     * @param entityName entity string to be encoded
     * @return encoded string
     */
    private String encodeHBaseEntity(String entityName) {
      try {
        return URLEncoder.encode(entityName, "ASCII");
      } catch (UnsupportedEncodingException e) {
        // this can never happen - we know that ASCII is a supported character set!
        throw new RuntimeException(e);
      }
    }
 
    public void initialize(HBaseDDLExecutorContext context) {
      LOG.info("Initializing executor with properties {}", context.getProperties());
      try {
        Configuration conf = context.getConfiguration();
        this.admin = new HBaseAdmin(conf);
 
        Configuration peerConf = generatePeerConfig(context);
        this.peerAdmin = new HBaseAdmin(peerConf);
      } catch (IOException e) {
        throw new RuntimeException("Failed to create the HBaseAdmin", e);
      }
    }
 
    private boolean hasNamespace(String name) throws IOException {
      Preconditions.checkArgument(admin != null, "HBaseAdmin should not be null");
      Preconditions.checkArgument(name != null, "Namespace should not be null.");
      try {
        admin.getNamespaceDescriptor(encodeHBaseEntity(name));
        return true;
      } catch (NamespaceNotFoundException e) {
        return false;
      }
    }
 
    public boolean createNamespaceIfNotExists(String name) throws IOException {
      Preconditions.checkArgument(name != null, "Namespace should not be null.");
      if (hasNamespace(name)) {
        return false;
      }
      NamespaceDescriptor namespaceDescriptor =
        NamespaceDescriptor.create(encodeHBaseEntity(name)).build();
      admin.createNamespace(namespaceDescriptor);
      peerAdmin.createNamespace(namespaceDescriptor);
      return true;
    }
 
    public void deleteNamespaceIfExists(String name) throws IOException {
      Preconditions.checkArgument(name != null, "Namespace should not be null.");
      if (hasNamespace(name)) {
        admin.deleteNamespace(encodeHBaseEntity(name));
        peerAdmin.deleteNamespace(encodeHBaseEntity(name));
      }
    }
 
    public void createTableIfNotExists(TableDescriptor descriptor, byte[][] splitKeys) throws IOException {
      createTableIfNotExists(getHTableDescriptor(descriptor), splitKeys);
    }
 
    private void createTableIfNotExists(HTableDescriptor htd, byte[][] splitKeys) throws IOException {
      if (admin.tableExists(htd.getName())) {
        return;
      }
 
      try {
        admin.createTable(htd, splitKeys);
        peerAdmin.createTable(htd, splitKeys);
        LOG.info("Table created '{}'", Bytes.toString(htd.getName()));
      } catch (TableExistsException e) {
        // table may exist because someone else is creating it at the same
        // time. But it may not be available yet, and opening it might fail.
        LOG.info("Table '{}' already exists.", Bytes.toString(htd.getName()), e);
      }
 
      // Wait for table to materialize
      try {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        long sleepTime = TimeUnit.MILLISECONDS.toNanos(5000L) / 10;
        sleepTime = sleepTime <= 0 ? 1 : sleepTime;
        do {
          if (admin.tableExists(htd.getName())) {
            LOG.debug("Table '{}' exists now. Assuming that another process concurrently created it.",
                      Bytes.toString(htd.getName()));
            return;
          } else {
            TimeUnit.NANOSECONDS.sleep(sleepTime);
          }
        } while (stopwatch.elapsedTime(TimeUnit.MILLISECONDS) < 5000L);
      } catch (InterruptedException e) {
        LOG.warn("Sleeping thread interrupted.");
      }
      LOG.error("Table '{}' does not exist after waiting {} ms. Giving up.", Bytes.toString(htd.getName()), 5000L);
    }
 
    public void enableTableIfDisabled(String namespace, String name) throws IOException {
      Preconditions.checkArgument(namespace != null, "Namespace should not be null");
      Preconditions.checkArgument(name != null, "Table name should not be null.");
 
      try {
        admin.enableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
        peerAdmin.enableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
      } catch (TableNotDisabledException e) {
        LOG.debug("Attempt to enable already enabled table {} in the namespace {}.", name, namespace);
      }
    }
 
    public void disableTableIfEnabled(String namespace, String name) throws IOException {
      Preconditions.checkArgument(namespace != null, "Namespace should not be null");
      Preconditions.checkArgument(name != null, "Table name should not be null.");
 
      try {
        admin.disableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
        peerAdmin.disableTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
      } catch (TableNotEnabledException e) {
        LOG.debug("Attempt to disable already disabled table {} in the namespace {}.", name, namespace);
      }
    }
 
    public void modifyTable(String namespace, String name, TableDescriptor descriptor) throws IOException {
      Preconditions.checkArgument(namespace != null, "Namespace should not be null");
      Preconditions.checkArgument(name != null, "Table name should not be null.");
      Preconditions.checkArgument(descriptor != null, "Descriptor should not be null.");
 
      HTableDescriptor htd = getHTableDescriptor(descriptor);
      admin.modifyTable(htd.getTableName(), htd);
      peerAdmin.modifyTable(htd.getTableName(), htd);
    }
 
    public void truncateTable(String namespace, String name) throws IOException {
      Preconditions.checkArgument(namespace != null, "Namespace should not be null");
      Preconditions.checkArgument(name != null, "Table name should not be null.");
 
      HTableDescriptor descriptor = admin.getTableDescriptor(TableName.valueOf(namespace, encodeHBaseEntity(name)));
      disableTableIfEnabled(namespace, name);
      deleteTableIfExists(namespace, name);
      createTableIfNotExists(descriptor, null);
    }
 
    public void deleteTableIfExists(String namespace, String name) throws IOException {
      Preconditions.checkArgument(namespace != null, "Namespace should not be null");
      Preconditions.checkArgument(name != null, "Table name should not be null.");
 
      admin.deleteTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
      peerAdmin.deleteTable(TableName.valueOf(namespace, encodeHBaseEntity(name)));
    }
 
    @Override
    public void grantPermissions(String s, String s1, Map<String, String> map) throws IOException {
      // no-op
    }
 
    public void close() throws IOException {
      if (admin != null) {
        admin.close();
      }
      if (peerAdmin != null) {
        peerAdmin.close();
      }
    }
 
    /**
     * Converts the {@link ColumnFamilyDescriptor} to the {@link HColumnDescriptor} for admin operations.
     * @param ns the namespace for the table
     * @param tableName the name of the table
     * @param descriptor descriptor of the column family
     * @return the instance of HColumnDescriptor
     */
    private static HColumnDescriptor getHColumnDesciptor(String ns, String tableName,
                                                         ColumnFamilyDescriptor descriptor) {
      HColumnDescriptor hFamily = new HColumnDescriptor(descriptor.getName());
      hFamily.setMaxVersions(descriptor.getMaxVersions());
      hFamily.setCompressionType(Compression.Algorithm.valueOf(descriptor.getCompressionType().name()));
      hFamily.setBloomFilterType(org.apache.hadoop.hbase.regionserver.BloomType.valueOf(
        descriptor.getBloomType().name()));
      for (Map.Entry<String, String> property : descriptor.getProperties().entrySet()) {
        hFamily.setValue(property.getKey(), property.getValue());
      }
      LOG.info("Setting replication scope to global for ns {}, table {}, cf {}", ns, tableName, descriptor.getName());
      hFamily.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
      return hFamily;
    }
 
    /**
     * Converts the {@link TableDescriptor} into corresponding {@link HTableDescriptor} for admin operations.
     * @param descriptor the table descriptor instance
     * @return the instance of HTableDescriptor
     */
    private static HTableDescriptor getHTableDescriptor(TableDescriptor descriptor) {
      TableName tableName = TableName.valueOf(descriptor.getNamespace(), descriptor.getName());
      HTableDescriptor htd = new HTableDescriptor(tableName);
      for (Map.Entry<String, ColumnFamilyDescriptor> family : descriptor.getFamilies().entrySet()) {
        htd.addFamily(getHColumnDesciptor(descriptor.getNamespace(), descriptor.getName(), family.getValue()));
      }
 
      for (Map.Entry<String, CoprocessorDescriptor> coprocessor : descriptor.getCoprocessors().entrySet()) {
        CoprocessorDescriptor cpd = coprocessor.getValue();
        try {
          htd.addCoprocessor(cpd.getClassName(), new Path(cpd.getPath()), cpd.getPriority(), cpd.getProperties());
        } catch (IOException e) {
          LOG.error("Error adding coprocessor.", e);
        }
      }
 
      for (Map.Entry<String, String> property : descriptor.getProperties().entrySet()) {
        htd.setValue(property.getKey(), property.getValue());
      }
      return htd;
    }
 
    /**
     * Generate the peer configuration which is used to perform DDL operations on the remote cluster using Admin
     * @param context instance of {@link HBaseDDLExecutorContext} with which the DDL executor is initialized
     * @return the {@link Configuration} to be used for DDL operations on the remote cluster
     */
    private static Configuration generatePeerConfig(HBaseDDLExecutorContext context) {
      Configuration peerConf = new Configuration();
      peerConf.clear();
 
      for (Map.Entry<String, String> entry : context.getProperties().entrySet()) {
        peerConf.set(entry.getKey(), entry.getValue());
      }
 
      StringWriter sw = new StringWriter();
      try {
        Configuration.dumpConfiguration(peerConf, sw);
        LOG.debug("PeerConfig - {}", sw);
      } catch (IOException e) {
        LOG.error("Error dumping config.", e);
      }
      return peerConf;
    }
  }


POM File
========
Corresponding ``pom.xml``. Configure the property ``hbase-client`` (currently ``1.0.0-cdh5.5.1``) below as appropriate:

.. parsed-literal::

  <?xml version="1.0" encoding="UTF-8"?>
  <project xmlns="http://maven.apache.org/POM/4.0.0"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
 
    <groupId>co.cask.cdap</groupId>
    <artifactId>HBaseDDLExecutorExtension</artifactId>
    <version>1.0-SNAPSHOT</version>
 
    <name>HBase DDL executor</name>
    <properties>
      <cdap.version>|release|</cdap.version>
      <slf4j.version>1.7.5</slf4j.version>
    </properties>
 
    <repositories>
      <repository>
        <id>sonatype</id>
        <url>https://oss.sonatype.org/content/groups/public</url>
      </repository>
      <repository>
        <id>apache.snapshots</id>
        <url>https://repository.apache.org/content/repositories/snapshots</url>
      </repository>
      <repository>
        <id>cloudera</id>
        <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
      </repository>
    </repositories>
 
    <dependencies>
      <dependency>
        <groupId>co.cask.cdap</groupId>
        <artifactId>cdap-hbase-spi</artifactId>
        <version>${cdap.version}</version>
        <scope>provided</scope>
      </dependency>
      <dependency>
        <groupId>org.apache.hbase</groupId>
        <artifactId>hbase-client</artifactId>
        <version>1.0.0-cdh5.5.1</version>
        <scope>provided</scope>
      </dependency>
      <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-api</artifactId>
        <version>${slf4j.version}</version>
      </dependency>
      <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>log4j-over-slf4j</artifactId>
        <version>${slf4j.version}</version>
      </dependency>
      <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>jcl-over-slf4j</artifactId>
        <version>${slf4j.version}</version>
      </dependency>
    </dependencies>
 
  </project>
