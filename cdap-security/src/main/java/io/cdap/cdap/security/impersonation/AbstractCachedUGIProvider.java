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

package io.cdap.cdap.security.impersonation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.security.AuthEnforceUtil;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * An abstract base class for {@link UGIProvider} that provides caching of {@link UGIWithPrincipal} containing the
 * {@link UserGroupInformation}.
 */
public abstract class AbstractCachedUGIProvider implements UGIProvider {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCachedUGIProvider.class);

  protected final CConfiguration cConf;
  private final LoadingCache<UGICacheKey, UGIWithPrincipal> ugiCache;
  private final OwnerAdmin ownerAdmin;

  protected AbstractCachedUGIProvider(CConfiguration cConf, OwnerAdmin ownerAdmin) {
    this.cConf = cConf;
    this.ownerAdmin = ownerAdmin;
    this.ugiCache = createUGICache(cConf);
  }

  /**
   * Creates a new {@link UGIWithPrincipal} based on the given {@link ImpersonationRequest}.
   */
  protected abstract UGIWithPrincipal createUGI(ImpersonationRequest impersonationRequest) throws AccessException;

  /**
   * Checks the {@link ImpersonationRequest} is an explore request and determine whether to cache the result or not
   */
  protected abstract boolean checkExploreAndDetermineCache(
    ImpersonationRequest impersonationRequest) throws AccessException;

  @Override
  public final UGIWithPrincipal getConfiguredUGI(ImpersonationRequest impersonationRequest)
    throws AccessException {
    try {
      UGIWithPrincipal ugi = impersonationRequest.getImpersonatedOpType().equals(ImpersonatedOpType.EXPLORE) ||
        impersonationRequest.getPrincipal() == null ?
        null : ugiCache.getIfPresent(new UGICacheKey(impersonationRequest));
      if (ugi != null) {
        return ugi;
      }
      boolean isCache = checkExploreAndDetermineCache(impersonationRequest);
      ImpersonationRequest tmpRequest = impersonationRequest;
      if (impersonationRequest.getEntityId() instanceof ProgramRunId) {
        ProgramId progId = ((ProgramRunId) impersonationRequest.getEntityId()).getParent();
        tmpRequest = new ImpersonationRequest(progId, impersonationRequest.getImpersonatedOpType());
      }
      ImpersonationInfo info = getPrincipalForEntity(tmpRequest);
      ImpersonationRequest newRequest = new ImpersonationRequest(impersonationRequest.getEntityId(),
                                                                 impersonationRequest.getImpersonatedOpType(),
                                                                 info.getPrincipal(), info.getKeytabURI());
      return isCache ? ugiCache.get(new UGICacheKey(newRequest)) : createUGI(newRequest);
    } catch (ExecutionException e) {
      throw AuthEnforceUtil.propagateAccessException(e);
    }
  }

  @VisibleForTesting
  void invalidCache() {
    ugiCache.invalidateAll();
    ugiCache.cleanUp();
  }

  private LoadingCache<UGICacheKey, UGIWithPrincipal> createUGICache(CConfiguration cConf) {
    long expirationMillis = cConf.getLong(Constants.Security.UGI_CACHE_EXPIRATION_MS);
    return CacheBuilder.newBuilder()
      .expireAfterWrite(expirationMillis, TimeUnit.MILLISECONDS)
      .build(new CacheLoader<UGICacheKey, UGIWithPrincipal>() {
        @Override
        public UGIWithPrincipal load(UGICacheKey key) throws Exception {
          return createUGI(key.getRequest());
        }
      });
  }

  private ImpersonationInfo getPrincipalForEntity(ImpersonationRequest request) throws AccessException {
    ImpersonationInfo impersonationInfo = SecurityUtil.createImpersonationInfo(ownerAdmin, cConf,
                                                                               request.getEntityId());
    LOG.debug("Obtained impersonation info: {} for entity {}", impersonationInfo, request.getEntityId());
    return impersonationInfo;
  }

  private static final class UGICacheKey {
    private final ImpersonationRequest request;

    UGICacheKey(ImpersonationRequest request) {
      this.request = request;
    }

    public ImpersonationRequest getRequest() {
      return request;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      UGICacheKey cachekey = (UGICacheKey) o;
      return Objects.equals(request.getPrincipal(), cachekey.getRequest().getPrincipal());
    }

    @Override
    public int hashCode() {
      return Objects.hash(request.getPrincipal());
    }
  }
}
