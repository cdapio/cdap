package com.continuuity.hive.server;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URLClassLoader;

/**
 * Runs HiveServer2 in isolated classloader.
 */
public class IsolatedHiveServer extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(IsolatedHiveServer.class);

  private final ClassLoader hiveClassLoader;
  private Class<?> hiveServerClass;
  private Object hiveServer2;

  public IsolatedHiveServer(ClassLoader hiveClassLoader) {
    this.hiveClassLoader = hiveClassLoader;
  }

  @Override
  protected void startUp() throws Exception {
    LOG.error("Classloader URLs = {}", Lists.newArrayList(((URLClassLoader) hiveClassLoader).getURLs()));
    Thread.currentThread().setContextClassLoader(hiveClassLoader);

    hiveServerClass = hiveClassLoader.loadClass("org.apache.hive.service.server.HiveServer2");
    Class<?> hiveConfClass = hiveClassLoader.loadClass("org.apache.hadoop.hive.conf.HiveConf");

    Configuration hiveConf = (Configuration) hiveConfClass.newInstance();
    logConfiguration(hiveConf);

    hiveServer2 = hiveServerClass.newInstance();
    Method initMethod = hiveServerClass.getMethod("init", hiveConfClass);
    initMethod.invoke(hiveServer2, hiveConf);

    Method startMethod = hiveServerClass.getMethod("start");
    startMethod.invoke(hiveServer2);
  }

  @Override
  protected void shutDown() throws Exception {
    Method stopMethod = hiveServerClass.getMethod("stop");
    stopMethod.invoke(hiveServer2);
  }

  private void logConfiguration(Configuration conf) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    conf.writeXml(bos);
    bos.close();
    LOG.error("HiveConf = {}", bos.toString("UTF-8"));
  }
}
