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

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Stream input format for use in hive queries and only hive queries. Will not work outside of hive.
 */
public class HiveStreamInputFormat implements InputFormat<Void, ObjectWritable> {

  @Override
  public InputSplit[] getSplits(JobConf conf, int numSplits) throws IOException {
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader newClassLoader = getClassLoader(contextClassLoader);
    Thread.currentThread().setContextClassLoader(newClassLoader);
    try {
      Class<?> aClass = newClassLoader.loadClass("co.cask.cdap.hive.stream.HiveStreamInputFormatDelegate");

      Object obj = aClass.newInstance();

      Method method = aClass.getDeclaredMethod("getSplits", JobConf.class, int.class);
      return (InputSplit[]) method.invoke(obj, conf, numSplits);
    } catch (ClassNotFoundException | InstantiationException | NoSuchMethodException
      | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  @Override
  public RecordReader<Void, ObjectWritable> getRecordReader(InputSplit split, JobConf conf, Reporter reporter)
    throws IOException {
    // TODO: in the task, which classloader loads the InputSplit object? Do we need to put the StreamInputSplit class
    // in the hive classpath? Otherwise, how will the framework load the class
    ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader newClassLoader = getClassLoader(contextClassLoader);
    Thread.currentThread().setContextClassLoader(newClassLoader);
    try {
      Class<?> aClass = newClassLoader.loadClass("co.cask.cdap.hive.stream.HiveStreamInputFormatDelegate");

      Object obj = aClass.newInstance();

      Method method = aClass.getDeclaredMethod("getRecordReader", InputSplit.class, JobConf.class, Reporter.class);
      return (RecordReader<Void, ObjectWritable>) method.invoke(obj, split, conf, reporter);
    } catch (ClassNotFoundException | InstantiationException | NoSuchMethodException
      | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    } finally {
      Thread.currentThread().setContextClassLoader(contextClassLoader);
    }
  }

  ClassLoader cl;

  // this caching fixed the issue in StreamRecordReader#init
  private ClassLoader getClassLoader(ClassLoader currentContextClassLoader) {
    if (cl == null) {
      cl = HiveClassLoader.getCL(currentContextClassLoader);
    }
    return cl;
  }
}
