/**
 * Copyright (c) 2012 to Continuuity Inc. All rights reserved.
 */
package com.continuuity.harness.queue;

import com.continuuity.harness.common.FileAggregator;
import com.continuuity.queue.IQueue;
import com.continuuity.queue.impl.HBaseQueue;
import com.continuuity.queue.impl.InMemoryQueues;
import com.google.inject.internal.Lists;
import etm.core.aggregation.BufferedTimedAggregator;
import etm.core.aggregation.RootAggregator;
import etm.core.configuration.BasicEtmConfigurator;
import etm.core.configuration.EtmManager;
import etm.core.configuration.EtmMonitorFactory;
import etm.core.monitor.EtmMonitor;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 *  Very simple queue Test Harnes Main.
 */
public class HarnessRunner {
  private static EtmMonitor monitor;
  
  public static void main(String[] args) {
    List<QueueAdapter> queueAdapterList = Lists.newArrayList();
    List<Producer> producerList = Lists.newArrayList();
    List<Consumer> consumerList = Lists.newArrayList();
    
    Options options = new Options();
    options.addOption("c", true, "Test configuration file");
    options.addOption("t", true, "Name of the test");
    options.addOption("q", true, "HBase ZK Quorum, if multiple hosts in qourum seperate by comma");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse( options, args );
    } catch(ParseException e) {
      System.err.println("Command line parsing error " + e.getLocalizedMessage());
      System.exit(-1);
    }
    
    if(! cmd.hasOption("c")) {
      System.err.println("No configuration file specified.");
      System.exit(-2);
    }
    
    if(! cmd.hasOption("t")) {
      System.err.println("No test name specified.");
      System.exit(-3);
    }

    String testName=cmd.getOptionValue("t");

    // Load the properties file
    Properties properties = new Properties();

    try {
      FileInputStream stream = new FileInputStream(cmd.getOptionValue("c"));
      properties.load(stream);
    } catch (FileNotFoundException e) {
      System.err.println("File " + args[0] + " not found");
      System.exit(-1);
    } catch (IOException e) {
      System.err.println("Failed " + e.getMessage());
      System.exit(-1);
    }

    String resultDir = properties.getProperty("result.dir", "/tmp");

    // Setup metric collector.
    BasicEtmConfigurator.configure(false, EtmMonitorFactory.bestAvailableTimer(),
      new BufferedTimedAggregator(new FileAggregator(new RootAggregator(), resultDir + "/" + testName + ".txt")));
    monitor = EtmManager.getEtmMonitor();
    monitor.start();

    try {

      String messageSize = properties.getProperty("message.size", "16");
      Integer msgSize = Integer.parseInt(messageSize);

      String messageCount = properties.getProperty("message.count", "1000");
      Integer msgCount = Integer.parseInt(messageCount);

      String producerCount = properties.getProperty("producer.count", "1");
      Integer pCount = Integer.parseInt(producerCount);

      String consumerCount = properties.getProperty("consumer.count", "1");
      Integer cCount = Integer.parseInt(consumerCount);

      String queueCount = properties.getProperty("queue.count", "1");
      Integer qCount = Integer.parseInt(queueCount);

      if(qCount < 0 ) {
        throw new Exception("Zero queue specified.");
      }

      String storageType = properties.getProperty("storage.engine", "hbase");
      IQueue queue = null;

      // If type specified is hbase, then initialize the necessary resources
      // to run.
      if("hbase".equals(storageType)) {
        Configuration conf =  HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", cmd.getOptionValue("q", "localhost"));
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(! admin.isTableAvailable(testName)) {
          HColumnDescriptor queueDescriptor = new HColumnDescriptor("QUEUE");
          HColumnDescriptor counterDescriptor = new HColumnDescriptor("COUNTER");
          HColumnDescriptor ackDescriptor = new HColumnDescriptor("ACKNOWLEDGEMENT");
          HTableDescriptor tableDescriptor = new HTableDescriptor(testName);
          tableDescriptor.addFamily(queueDescriptor);
          tableDescriptor.addFamily(counterDescriptor);
          tableDescriptor.addFamily(ackDescriptor);
          admin.createTable(tableDescriptor);
        }
        queue = new HBaseQueue(conf, testName );
      } else if("memory".equals(storageType)) {
        queue = new InMemoryQueues();
      } else {
        System.err.println("Unknown Storage engine specified.");
      }

      // Create queues needed for test
      for(int i = 0; i < qCount; ++i) {
        QueueAdapter adapter = new QueueAdapter(String.valueOf(i), queue);
        queueAdapterList.add(adapter);
      }

      // Create producer(s)
      for(int i = 0; i < pCount; ++i) {
        QueueAdapter adapter = queueAdapterList.get( i % qCount);
        Producer producer = new Producer(adapter, msgSize.intValue(),
          msgCount/qCount);
        producerList.add(producer);
        producer.start();
      }

      // Create consumer(s)
      for(int i = 0; i < cCount; ++i) {
        QueueAdapter adapter = queueAdapterList.get( i % qCount);
        Consumer consumer = new Consumer(""+i, adapter, msgCount/qCount);
        consumerList.add(consumer);
        consumer.start();
      }

      // Wait for all the producers to finish
      for(int i = 0; i < pCount; ++i) {
        try {
          producerList.get(i).join();
        } catch (InterruptedException e) {
          System.err.println("Warning: " + e.getMessage());
        }
      }

      // Wait for all the consumers to finish.
      // Consumers will not complete till they see specified number of
      // messages.
      for(int i = 0; i < cCount; ++i) {
        try {
          consumerList.get(i).join();
        } catch (InterruptedException e) {
          System.err.println("Warning: " + e.getMessage());
        }
      }
    } catch (Exception e) {
      System.err.println("Found error : " + e.getMessage());
    } finally {
      monitor.stop();
    }
  }
}
