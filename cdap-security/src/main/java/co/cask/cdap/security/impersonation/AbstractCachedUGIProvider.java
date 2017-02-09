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

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An abstract base class for {@link UGIProvider} that provides caching of {@link UserGroupInformation}.
 */
public abstract class AbstractCachedUGIProvider implements UGIProvider {

  private final LoadingCache<ImpersonationInfo, UserGroupInformation> ugiCache;

  protected AbstractCachedUGIProvider(CConfiguration cConf) {
    this.ugiCache = createUGICache(cConf);
  }

  /**
   * Creates a new {@link UserGroupInformation} based on the given {@link ImpersonationInfo}.
   */
  protected abstract UserGroupInformation createUGI(ImpersonationInfo impersonationInfo) throws IOException;

  @Override
  public final UserGroupInformation getConfiguredUGI(ImpersonationInfo impersonationInfo) throws IOException {
    try {
      return ugiCache.get(impersonationInfo);
    } catch (ExecutionException e) {
      // Get the root cause of the failure
      Throwable cause = Throwables.getRootCause(e);
      // Propagate if the cause is an IOException or RuntimeException
      Throwables.propagateIfPossible(cause, IOException.class);
      // Otherwise always wrap it with IOException
      throw new IOException(cause);
    }
  }

  @VisibleForTesting
  void invalidCache() {
    ugiCache.invalidateAll();
    ugiCache.cleanUp();
  }

  private LoadingCache<ImpersonationInfo, UserGroupInformation> createUGICache(CConfiguration cConf) {
    long expirationMillis = cConf.getLong(Constants.Security.UGI_CACHE_EXPIRATION_MS);
    return CacheBuilder.newBuilder()
      .expireAfterWrite(expirationMillis, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<ImpersonationInfo, UserGroupInformation>() {
        @Override
        public UserGroupInformation load(ImpersonationInfo impersonationInfo) throws Exception {
          return createUGI(impersonationInfo);
        }
      });
  }
}
