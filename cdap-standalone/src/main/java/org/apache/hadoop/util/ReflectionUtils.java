/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package org.apache.hadoop.util;

import io.cdap.cdap.internal.app.runtime.plugin.PluginClassLoader;
import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * General reflection utils.
 * This is a copy of ReflectionUtils in hadoop-common-2.3.0.jar.
 * This copy has been modified not to cache constructors in method {@link #newInstance(Class, Configuration)}.
 * Caching of constructors causes {@link PluginClassLoader}s to be retained when running that use plugins,
 * thus causing OOM.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReflectionUtils {

  private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
  private static volatile SerializationFactory serialFactory;

  /**
   * Check and set 'configuration' if necessary.
   *
   * @param theObject object for which to set configuration
   * @param conf Configuration
   */
  public static void setConf(Object theObject, Configuration conf) {
    if (conf != null) {
      if (theObject instanceof Configurable) {
        ((Configurable) theObject).setConf(conf);
      }
      setJobConf(theObject, conf);
    }
  }

  /**
   * This code is to support backward compatibility and break the compile  
   * time dependency of core on mapred.
   * This should be made deprecated along with the mapred package HADOOP-1230. 
   * Should be removed when mapred package is removed.
   */
  private static void setJobConf(Object theObject, Configuration conf) {
    //If JobConf and JobConfigurable are in classpath, AND
    //theObject is of type JobConfigurable AND
    //conf is of type JobConf then
    //invoke configure on theObject
    try {
      Class<?> jobConfClass =
        conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConf");
      if (jobConfClass == null) {
        return;
      }

      Class<?> jobConfigurableClass =
        conf.getClassByNameOrNull("org.apache.hadoop.mapred.JobConfigurable");
      if (jobConfigurableClass == null) {
        return;
      }
      if (jobConfClass.isAssignableFrom(conf.getClass()) &&
        jobConfigurableClass.isAssignableFrom(theObject.getClass())) {
        Method configureMethod =
          jobConfigurableClass.getMethod("configure", jobConfClass);
        configureMethod.invoke(theObject, conf);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error in configuring object", e);
    }
  }

  /** Create an object for the given class and initialize it from conf
   *
   * @param theClass class of which an object is created
   * @param conf Configuration
   * @return a new object
   */
  @SuppressWarnings("unchecked")
  public static <T> T newInstance(Class<T> theClass, Configuration conf) {
    T result;
    try {
      Constructor<T> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
      meth.setAccessible(true);
      result = meth.newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    setConf(result, conf);
    return result;
  }

  private static ThreadMXBean threadBean =
    ManagementFactory.getThreadMXBean();

  public static void setContentionTracing(boolean val) {
    threadBean.setThreadContentionMonitoringEnabled(val);
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  /**
   * Print all of the thread's information and stack traces.
   *
   * @param stream the stream to
   * @param title a string title for the stack trace
   */
  public static synchronized void printThreadInfo(PrintWriter stream,
                                                  String title) {
    final int stackDepth = 20;
    boolean contention = threadBean.isThreadContentionMonitoringEnabled();
    long[] threadIds = threadBean.getAllThreadIds();
    stream.println("Process Thread Dump: " + title);
    stream.println(threadIds.length + " active threads");
    for (long tid: threadIds) {
      ThreadInfo info = threadBean.getThreadInfo(tid, stackDepth);
      if (info == null) {
        stream.println("  Inactive");
        continue;
      }
      stream.println("Thread " +
                       getTaskName(info.getThreadId(),
                                   info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      stream.println("  State: " + state);
      stream.println("  Blocked count: " + info.getBlockedCount());
      stream.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        stream.println("  Blocked time: " + info.getBlockedTime());
        stream.println("  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        stream.println("  Waiting on " + info.getLockName());
      } else  if (state == Thread.State.BLOCKED) {
        stream.println("  Blocked on " + info.getLockName());
        stream.println("  Blocked by " +
                         getTaskName(info.getLockOwnerId(),
                                     info.getLockOwnerName()));
      }
      stream.println("  Stack:");
      for (StackTraceElement frame: info.getStackTrace()) {
        stream.println("    " + frame.toString());
      }
    }
    stream.flush();
  }

  private static long previousLogTime;

  /**
   * Log the current thread stacks at INFO level.
   * @param log the logger that logs the stack trace
   * @param title a descriptive title for the call stacks
   * @param minInterval the minimum time from the last 
   */
  public static void logThreadInfo(Log log,
                                   String title,
                                   long minInterval) {
    boolean dumpStack = false;
    if (log.isInfoEnabled()) {
      synchronized (ReflectionUtils.class) {
        long now = Time.now();
        if (now - previousLogTime >= minInterval * 1000) {
          previousLogTime = now;
          dumpStack = true;
        }
      }
      if (dumpStack) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        printThreadInfo(new PrintWriter(buffer), title);
        log.info(buffer.toString());
      }
    }
  }

  /**
   * Return the correctly-typed {@link Class} of the given object.
   *
   * @param o object whose correctly-typed <code>Class</code> is to be obtained
   * @return the correctly typed <code>Class</code> of the given object.
   */
  @SuppressWarnings("unchecked")
  public static <T> Class<T> getClass(T o) {
    return (Class<T>) o.getClass();
  }

  /**
   * A pair of input/output buffers that we use to clone writables.
   */
  private static class CopyInCopyOutBuffer {
    DataOutputBuffer outBuffer = new DataOutputBuffer();
    DataInputBuffer inBuffer = new DataInputBuffer();
    /**
     * Move the data from the output buffer to the input buffer.
     */
    void moveData() {
      inBuffer.reset(outBuffer.getData(), outBuffer.getLength());
    }
  }

  /**
   * Allocate a buffer for each thread that tries to clone objects.
   */
  private static ThreadLocal<CopyInCopyOutBuffer> cloneBuffers
    = new ThreadLocal<CopyInCopyOutBuffer>() {
    @Override
    protected synchronized CopyInCopyOutBuffer initialValue() {
      return new CopyInCopyOutBuffer();
    }
  };

  private static SerializationFactory getFactory(Configuration conf) {
    if (serialFactory == null) {
      serialFactory = new SerializationFactory(conf);
    }
    return serialFactory;
  }

  /**
   * Make a copy of the writable object using serialization to a buffer
   * @param src the object to copy from
   * @param dst the object to copy into, which is destroyed
   * @return dst param (the copy)
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static <T> T copy(Configuration conf,
                           T src, T dst) throws IOException {
    CopyInCopyOutBuffer buffer = cloneBuffers.get();
    buffer.outBuffer.reset();
    SerializationFactory factory = getFactory(conf);
    Class<T> cls = (Class<T>) src.getClass();
    Serializer<T> serializer = factory.getSerializer(cls);
    serializer.open(buffer.outBuffer);
    serializer.serialize(src);
    buffer.moveData();
    Deserializer<T> deserializer = factory.getDeserializer(cls);
    deserializer.open(buffer.inBuffer);
    dst = deserializer.deserialize(dst);
    return dst;
  }

  @Deprecated
  public static void cloneWritableInto(Writable dst,
                                       Writable src) throws IOException {
    CopyInCopyOutBuffer buffer = cloneBuffers.get();
    buffer.outBuffer.reset();
    src.write(buffer.outBuffer);
    buffer.moveData();
    dst.readFields(buffer.inBuffer);
  }

  /**
   * Gets all the declared fields of a class including fields declared in
   * superclasses.
   */
  public static List<Field> getDeclaredFieldsIncludingInherited(Class<?> clazz) {
    List<Field> fields = new ArrayList<Field>();
    while (clazz != null) {
      for (Field field : clazz.getDeclaredFields()) {
        fields.add(field);
      }
      clazz = clazz.getSuperclass();
    }

    return fields;
  }

  /**
   * Gets all the declared methods of a class including methods declared in
   * superclasses.
   */
  public static List<Method> getDeclaredMethodsIncludingInherited(Class<?> clazz) {
    List<Method> methods = new ArrayList<Method>();
    while (clazz != null) {
      for (Method method : clazz.getDeclaredMethods()) {
        methods.add(method);
      }
      clazz = clazz.getSuperclass();
    }

    return methods;
  }
}
