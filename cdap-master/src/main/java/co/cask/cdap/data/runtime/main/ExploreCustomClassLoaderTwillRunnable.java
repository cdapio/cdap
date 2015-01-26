/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.data.runtime.main;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.CompoundEnumeration;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 * Used to load a {@link ExploreServiceTwillRunnable} using {@link CustomResourcesClassLoader}. This gives precedence 
 * to config files bundled with the application over config files in the classpath used to start the application.
 * This is required as on some clusters the classpath used to start a container contains a stripped down version of 
 * Hadoop config files.
 */
public class ExploreCustomClassLoaderTwillRunnable extends AbstractTwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(ExploreCustomClassLoaderTwillRunnable.class);
  private static final String CLASS_NAME = ExploreServiceTwillRunnable.class.getName();
  
  private TwillRunnable twillRunnable;
  private ClassLoader customClassLoader;

  public ExploreCustomClassLoaderTwillRunnable(AbstractTwillRunnable twillRunnable) {
    this.twillRunnable = twillRunnable;
  }

  @Override
  public TwillRunnableSpecification configure() {
    return twillRunnable.configure();
  }

  @Override
  public void initialize(TwillContext context) {
    super.initialize(context);
    
    if (getClass().getClassLoader() instanceof URLClassLoader) {
      LOG.info("Using CustomResourcesClassLoader to load config files bundled with application.");
      customClassLoader =
        new CustomResourcesClassLoader(((URLClassLoader) getClass().getClassLoader()).getURLs(),
                                       getClass().getClassLoader());
    } else {
      LOG.warn("Classloader is not URLCLassLoader, config files bundled with application might not be loaded.");
      customClassLoader = getClass().getClassLoader();
    }
    
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);

    try {
      @SuppressWarnings("unchecked") 
      Class<? extends TwillRunnable> twillRunnableClass = 
        (Class<? extends TwillRunnable>) customClassLoader.loadClass(CLASS_NAME);
      twillRunnable = newInstance(twillRunnableClass);
      twillRunnable.initialize(context);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void handleCommand(Command command) throws Exception {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      twillRunnable.handleCommand(command);
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void stop() {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      twillRunnable.stop();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void destroy() {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      twillRunnable.destroy();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @Override
  public void run() {
    ClassLoader previousClassLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(customClassLoader);
    try {
      twillRunnable.run();
    } finally {
      Thread.currentThread().setContextClassLoader(previousClassLoader);
    }
  }

  @VisibleForTesting
  static TwillRunnable newInstance(Class<? extends TwillRunnable> twillRunnableClass)
    throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<? extends TwillRunnable> constructor = 
      twillRunnableClass.getConstructor(String.class, String.class, String.class);
    return constructor.newInstance(null, null, null);
  }
  
  /**
   * A custom {@link ClassLoader} used to load config files bundled with a {@link TwillRunnable}.
   */
  @VisibleForTesting
  static class CustomResourcesClassLoader extends URLClassLoader {
    private final ClassLoader parent;

    public CustomResourcesClassLoader(URL[] urls, ClassLoader parent) {
      super(urls, ClassLoader.getSystemClassLoader().getParent());
      this.parent = parent;
    }

    @Override
    public URL getResource(String s) {
      URL url = super.findResource(s);
      if (url != null) {
        return url;
      }
      return parent.getResource(s);
    }

    @Override
    public Enumeration<URL> getResources(String s) throws IOException {
      Enumeration<URL> resources = super.findResources(s);
      return resources == null ? parent.getResources(s) :
        new CompoundEnumeration<URL>(new Enumeration[] {resources, parent.getResources(s)});
    }

    @Override
    public InputStream getResourceAsStream(String s) {
      InputStream in = super.getResourceAsStream(s);
      if (in != null) {
        return in;
      }
      return parent.getResourceAsStream(s);
    }

    @Override
    public Class<?> loadClass(String s) throws ClassNotFoundException {
      // Load all Twill API classes from parent classloader, since they are already loaded by parent classloader.
      if (s.startsWith("org.apache.twill.api.")) {
        return parent.loadClass(s);
      }

      return super.loadClass(s);
    }
  }
}
