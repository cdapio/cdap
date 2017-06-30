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

import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.ImpersonatedOpType;
import co.cask.cdap.common.kerberos.ImpersonationInfo;
import co.cask.cdap.common.kerberos.ImpersonationRequest;
import co.cask.cdap.common.kerberos.OwnerAdmin;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.kerberos.UGIWithPrincipal;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.element.EntityType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
  private final NamespaceQueryAdmin namespaceQueryAdmin;

  protected AbstractCachedUGIProvider(CConfiguration cConf, OwnerAdmin ownerAdmin,
                                      NamespaceQueryAdmin namespaceQueryAdmin) {
    this.cConf = cConf;
    this.ownerAdmin = ownerAdmin;
    this.ugiCache = createUGICache(cConf);
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  /**
   * Creates a new {@link UGIWithPrincipal} based on the given {@link ImpersonationRequest}.
   */
  protected abstract UGIWithPrincipal createUGI(ImpersonationRequest impersonationRequest) throws IOException;

  @Override
  public UGIWithPrincipal getConfiguredUGI(ImpersonationRequest impersonationRequest) throws IOException {
    try {
      UGIWithPrincipal principal = impersonationRequest.getPrincipal() == null ?
        null : ugiCache.getIfPresent(new UGICacheKey(impersonationRequest));
      if (principal != null) {
        return principal;
      }
      verifyExploreOperation(impersonationRequest);
      ImpersonationInfo info = getPrincipalForEntity(impersonationRequest);
      return ugiCache.get(new UGICacheKey(new ImpersonationRequest(impersonationRequest.getEntityId(),
                                                                   impersonationRequest.getImpersonatedOpType(),
                                                                   info.getPrincipal(), info.getKeytabURI())));
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

  private ImpersonationInfo getPrincipalForEntity(ImpersonationRequest request) throws IOException {
    ImpersonationInfo impersonationInfo = SecurityUtil.createImpersonationInfo(ownerAdmin, cConf,
                                                                               request.getEntityId());
    LOG.debug("Obtained impersonation info: {} for entity {}", impersonationInfo, request.getEntityId());
    return impersonationInfo;
  }

  private void verifyExploreOperation(ImpersonationRequest impersonationRequest) throws IOException {
    if (impersonationRequest.getEntityId().getEntityType().equals(EntityType.NAMESPACE) &&
      impersonationRequest.getImpersonatedOpType().equals(ImpersonatedOpType.EXPLORE)) {
      // CDAP-8355 If the operation being impersonated is an explore query then check if the namespace configuration
      // specifies that it can be impersonated with the namespace owner.
      // This is done here rather than in the get getConfiguredUGI because the getConfiguredUGI will be called at
      // remote location as in RemoteUGIProvider. Looking up namespace meta there will make a trip to master followed
      // by one to get the credentials. Hence do it here in the DefaultUGIProvider which is running in master.
      // Although, this will cause cache miss for explore implementation if the namespace config doesn't allow
      // impersonating the namespace owner for explore queries but that a trade-off to avoid multiple remote calls in
      // more prominent calls.
      try {
        NamespaceConfig nsConfig =
          namespaceQueryAdmin.get(impersonationRequest.getEntityId().getNamespaceId()).getConfig();
        if (!nsConfig.isExploreAsPrincipal()) {
          throw new FeatureDisabledException(FeatureDisabledException.Feature.EXPLORE,
                                             NamespaceConfig.class.getSimpleName() + " of " +
                                               impersonationRequest.getEntityId(),
                                             NamespaceConfig.EXPLORE_AS_PRINCIPAL, String.valueOf(true));
        }

      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private static final class UGICacheKey {
    private final ImpersonationRequest request;

    UGICacheKey(ImpersonationRequest request) {
      this.request = request;
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

    public ImpersonationRequest getRequest() {
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request.getPrincipal());
    }
  }
}
