/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.common.lang.ClassLoaders;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that helps with creating and initializing objects from, based upon an attribute in
 * a {@link Configuration}.
 */
public final class WrapperUtil {
  private static final Logger LOG = LoggerFactory.getLogger(WrapperUtil.class);

  private WrapperUtil() {
  }

  // instantiates and initializes (if its a ProgramLifeCycle) the class specified by the attribute
  static <T> T createDelegate(Configuration conf, String attrClass) {
    String delegateClassName = conf.get(attrClass);
    Class<?> delegateClass = conf.getClassByNameOrNull(delegateClassName);
    Preconditions.checkNotNull(delegateClass, "Class could not be found: ", delegateClassName);
    T delegate = (T) ReflectionUtils.newInstance(delegateClass, conf);

    if (!(delegate instanceof ProgramLifecycle)) {
      return delegate;
    }

    MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(conf);
    BasicMapReduceTaskContext basicMapReduceContext = classLoader.getTaskContextProvider().get(conf);
    ClassLoader programClassLoader = classLoader.getProgramClassLoader();

    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(programClassLoader);
    try {
      ProgramLifecycle programLifecycle = (ProgramLifecycle) delegate;
      programLifecycle.initialize(new MapReduceLifecycleContext(basicMapReduceContext));

      // register it so that its destroy method can get called when the BasicMapReduceTaskContext is closed
      basicMapReduceContext.registerProgramLifecycle(programLifecycle);
      return delegate;
    } catch (Exception e) {
      LOG.error("Failed to initialize delegate with {}", basicMapReduceContext, e);
      throw Throwables.propagate(e);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  // copies the value from src key to destination key
  // return true if the src key had a corresponding value
  static boolean setIfDefined(Job job, String srcKey, String destinationKey) {
    // NOTE: we don't use job.getXClass or conf.getClass as we don't need to load user class here
    Configuration conf = job.getConfiguration();
    String srcVal = conf.get(srcKey);
    if (srcVal != null) {
      conf.set(destinationKey, srcVal);
      return true;
    }
    return false;
  }
}
