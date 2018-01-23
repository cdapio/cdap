/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

package co.cask.cdap.hive.stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;

/**
 * SerDe to deserialize Stream Events. It MUST implement the deprecated SerDe interface instead of extending the
 * abstract SerDe class, otherwise we get ClassNotFound exceptions on cdh4.x.
 */
@SuppressWarnings("deprecation")
public class StreamSerDe implements SerDe {
  private static final Logger LOG = LoggerFactory.getLogger(StreamSerDe.class);

  private ObjectInspector inspector;
  // StreamSerDeInitializer
  private Object obj;

  // initialize gets called multiple times by Hive. It may seem like a good idea to put additional settings into
  // the conf, but be very careful when doing so. If there are multiple hive tables involved in a query, initialize
  // for each table is called before input splits are fetched for any table. It is therefore not safe to put anything
  // the input format may need into conf in this method. Rather, use StorageHandler's method to place needed config
  // into the properties map there, which will get passed here and also copied into the job conf for the input
  // format to consume.
  @Override
  public void initialize(Configuration conf, Properties properties) throws SerDeException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader newClassLoader = HiveClassLoader.getCL(contextClassLoader);
    Thread.currentThread().setContextClassLoader(newClassLoader);
    try {
      Class<?> aClass = newClassLoader.loadClass("co.cask.cdap.hive.stream.StreamSerDeInitializer");

      this.obj = aClass.newInstance();

      Method method = aClass.getDeclaredMethod("initialize", Configuration.class, Properties.class);
      method.invoke(obj, conf, properties);

      Field inspectorField = aClass.getDeclaredField("inspector");
      inspectorField.setAccessible(true);
      this.inspector = (ObjectInspector) inspectorField.get(obj);

    } catch (ClassNotFoundException | InstantiationException | NoSuchMethodException
      | IllegalAccessException | InvocationTargetException | NoSuchFieldException e) {
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return Text.class;
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException {
    // should not be writing to streams through this
    throw new SerDeException("Stream serialization through Hive is not supported.");
  }

  @Override
  public SerDeStats getSerDeStats() {
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException {
    try {
      Method method = obj.getClass().getDeclaredMethod("deserialize", Writable.class);
      return method.invoke(obj, writable);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return inspector;
  }
}
