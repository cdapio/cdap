/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

package co.cask.cdap.security.impersonation;

import com.google.common.base.Throwables;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;

/**
 * Utility functions for impersonation.
 */
public final class ImpersonationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonationUtils.class);

  private ImpersonationUtils() { }

  /**
   * Helper function, to unwrap any exceptions that were wrapped
   * by {@link UserGroupInformation#doAs(PrivilegedExceptionAction)}
   */
  public static <T> T doAs(UserGroupInformation ugi, final Callable<T> callable) throws Exception {
    try {
      return ugi.doAs(new PrivilegedExceptionAction<T>() {
        @Override
        public T run() throws Exception {
          return callable.call();
        }
      });
    } catch (UndeclaredThrowableException e) {
      // UserGroupInformation#doAs will wrap any checked exceptions, so unwrap and rethrow here
      Throwable wrappedException = e.getUndeclaredThrowable();
      Throwables.propagateIfPossible(wrappedException);

      if (wrappedException instanceof Exception) {
        throw (Exception) wrappedException;
      }
      // since PrivilegedExceptionAction#run can only throw Exception (besides runtime exception),
      // this should never happen
      LOG.warn("Unexpected exception while executing callable as {}.",
               ugi.getUserName(), wrappedException);
      throw Throwables.propagate(wrappedException);
    }
  }
}
