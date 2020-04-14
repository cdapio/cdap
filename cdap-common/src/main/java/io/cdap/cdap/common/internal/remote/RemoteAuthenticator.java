/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.common.base.Objects;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * This interface expose methods for authenticating HTTP calls.
 * It is used to set the {@code Authorization} header in the form of
 * <pre>
 * {@code Authorization: type credentials}
 * </pre>
 */
public abstract class RemoteAuthenticator {

  /**
   * System property name for the class name of the {@link RemoteAuthenticator}.
   */
  private static final String DEFAULT_AUTHENTICATOR_CLASS = "cdap.remote.authenticator.class";
  private static volatile RemoteAuthenticator defaultAuthenticator;

  /**
   * Sets the default {@link RemoteAuthenticator} to be used.
   *
   * @param authenticator the default {@link RemoteAuthenticator} instance.
   *                      If it is {@code null}, it removes the default.
   * @return the {@link RemoteAuthenticator} before the change
   */
  @Nullable
  public static synchronized RemoteAuthenticator setDefaultAuthenticator(@Nullable RemoteAuthenticator authenticator) {
    RemoteAuthenticator oldAuthenticator = defaultAuthenticator;
    defaultAuthenticator = authenticator;
    return oldAuthenticator;
  }

  /**
   * Returns the default {@link RemoteAuthenticator} or {@code null} if there is no default available.
   * A default {@link RemoteAuthenticator} can be set via the
   * {@link #setDefaultAuthenticator(RemoteAuthenticator)} method.
   * <p/>
   * If none was set using the {@link #setDefaultAuthenticator(RemoteAuthenticator)} method prior to this call,
   * the class name defined by the system property {@link #DEFAULT_AUTHENTICATOR_CLASS} will be used to load
   * the {@link RemoteAuthenticator} class from the current context classloader.
   */
  @Nullable
  public static RemoteAuthenticator getDefaultAuthenticator() {
    RemoteAuthenticator authenticator = defaultAuthenticator;
    if (authenticator != null) {
      return authenticator;
    }

    // Try to set the default based on system property.
    String className = System.getProperty(DEFAULT_AUTHENTICATOR_CLASS);
    if (className == null) {
      return null;
    }
    ClassLoader cl = Objects.firstNonNull(Thread.currentThread().getContextClassLoader(),
                                          RemoteAuthenticator.class.getClassLoader());
    try {
      Class<?> cls = cl.loadClass(className);
      if (!RemoteAuthenticator.class.isAssignableFrom(cls)) {
        throw new IllegalArgumentException("Class " + cls.getName() + " loaded using classloader " + cl
                                             + " is not an instance of " + RemoteAuthenticator.class.getName());
      }
      setDefaultAuthenticator((RemoteAuthenticator) cls.newInstance());
      return defaultAuthenticator;
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Class " + className + " is not loadable using classloader " + cl, e);
    } catch (IllegalAccessException | InstantiationException e) {
      throw new IllegalArgumentException("Failed to instantiate class " + className
                                           + " as an instance of " + RemoteAuthenticator.class.getName());
    }
  }

  /**
   * Returns the type of the authentication.
   */
  public abstract String getType() throws IOException;

  /**
   * Returns the credentials for the authentication. It must be conformed to the requirement of the type returned by
   * {@link #getType()}.
   */
  public abstract String getCredentials() throws IOException;
}
