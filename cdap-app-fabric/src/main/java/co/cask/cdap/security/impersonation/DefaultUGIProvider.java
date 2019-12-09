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

import co.cask.cdap.app.runtime.ProgramRuntimeService;
import co.cask.cdap.app.runtime.ProgramRuntimeService.RuntimeInfo;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.FeatureDisabledException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespaceQueryAdmin;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.common.utils.FileUtils;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.TwillRunProgramId;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Provides a UGI by logging in with a keytab file for that user.
 */
public class DefaultUGIProvider extends AbstractCachedUGIProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultUGIProvider.class);

  private final LocationFactory locationFactory;
  private final File tempDir;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  protected final Store store;
  private static final String RUNTIME_ARG_KEYTAB = "pipeline.keytab.path";
  private static final String RUNTIME_ARG_PRINCIPAL = "pipeline.principal.name";
  private static final Gson gson = new Gson();
  private final ProgramRuntimeService runtimeService;
 
  @Inject
  DefaultUGIProvider(CConfiguration cConf, LocationFactory locationFactory, OwnerAdmin ownerAdmin,
                     NamespaceQueryAdmin namespaceQueryAdmin, Store store,
                     ProgramRuntimeService runtimeService) {
    super(cConf, ownerAdmin);
    this.locationFactory = locationFactory;
    this.tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                            cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.store = store;
    this.runtimeService = runtimeService;
  }

  /**
   * On master side, we can cache the explore request
   */
  @Override
  protected boolean checkExploreAndDetermineCache(ImpersonationRequest impersonationRequest) throws IOException {
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
    return true;
  }

  /**
   * Resolves the {@link UserGroupInformation} for a given user, performing any keytab localization, if necessary.
   *
   * @return a {@link UserGroupInformation}, based upon the information configured for a particular user
   * @throws IOException if there was any IOException during localization of the keytab
   */
  @Override
  protected UGIWithPrincipal createUGI(ImpersonationRequest impersonationRequest) throws IOException {
    Map<String, String> properties = getRuntimeImpersonationProperties(impersonationRequest.getEntityId());
      
    boolean isUserImpersonationEnabled = false;
    if ((properties != null) &&
          (properties.containsKey(SystemArguments.USER_IMPERSONATION_ENABLED))) {
      isUserImpersonationEnabled = true;
    }
      
    /*
     *  If user impersonation is enabled, identity of user will be propagated to downstream systems, else
     *  we will use Application runtime or Namespace level impersonation
     *  
     *  Get runtime arguments containing keytab and principal from runtime arguments if present
     */
    
    ImpersonationRequest updatedRequest = impersonationRequest;
    if ((!isUserImpersonationEnabled) &&
          (properties.containsKey(RUNTIME_ARG_KEYTAB)) &&
          (properties.containsKey(RUNTIME_ARG_PRINCIPAL))) {
      String keytab = properties.get(RUNTIME_ARG_KEYTAB);
      String principal = properties.get(RUNTIME_ARG_PRINCIPAL);
      updatedRequest = new ImpersonationRequest(impersonationRequest.getEntityId(),
              impersonationRequest.getImpersonatedOpType(), keytab, principal);
      LOG.debug("Using runtime config principal: " + principal + ", keytab = " + keytab);
    }
      
    String loggedInUser = getImpersonatedUser(updatedRequest);
    return createUGI(updatedRequest, loggedInUser);
  }
  
  protected UGIWithPrincipal createUGI(ImpersonationRequest impersonationRequest, 
          String loggedInUser) throws IOException {
      
    NamespacedEntityId programId = impersonationRequest.getEntityId();
    
    // no need to login from keytab if the current UGI already matches
    String configuredPrincipalShortName = new KerberosName(impersonationRequest.getPrincipal()).getShortName();
    if (UserGroupInformation.getCurrentUser().getShortUserName().equals(configuredPrincipalShortName)) {
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      UserGroupInformation proxyUgi = getProxyUGI(ugi, programId, loggedInUser);   
      return new UGIWithPrincipal(impersonationRequest.getPrincipal(), proxyUgi);
    }

    URI keytabURI = URI.create(impersonationRequest.getKeytabURI());
    boolean isKeytabLocal = keytabURI.getScheme() == null || "file".equals(keytabURI.getScheme());

    File localKeytabFile = isKeytabLocal ?
      new File(keytabURI.getPath()) : localizeKeytab(locationFactory.create(keytabURI));
    try {
      String expandedPrincipal = SecurityUtil.expandPrincipal(impersonationRequest.getPrincipal());
      LOG.debug("Logging in as: principal={}, keytab={}", expandedPrincipal, localKeytabFile);

      // Note: if the keytab file is not local then then localizeKeytab function call above which tries to localize the
      // keytab from HDFS will throw IOException if the file does not exists or is not readable. Where as if the file
      // is local then the localizeKeytab function is not called so its important that we throw IOException if the local
      // keytab file is not readable to ensure that the client gets the same exception in both the modes.
      if (!Files.isReadable(localKeytabFile.toPath())) {
        throw new IOException(String.format("Keytab file is not a readable file: %s", localKeytabFile));
      }

      UserGroupInformation loggedInUGI;
      try {
        UserGroupInformation tmpUgi =
              UserGroupInformation.loginUserFromKeytabAndReturnUGI(expandedPrincipal,
              localKeytabFile.getAbsolutePath());
        loggedInUGI = getProxyUGI(tmpUgi, programId, loggedInUser);
     
        LOG.debug("loggedInUGI {} , realUser {}, userShortName {} , userName {} , loginUser {}", 
             loggedInUGI, loggedInUGI.getRealUser(), loggedInUGI.getShortUserName(), loggedInUGI.getUserName(),
             loggedInUGI.getLoginUser());
         
         
      } catch (Exception e) {
        // rethrow the exception with additional information tagged, so the user knows which principal/keytab is
        // not working
        throw new IOException(String.format("Failed to login for principal=%s, keytab=%s. Check that the principal " +
                                              "was not deleted and that the keytab is still valid.",
                                            expandedPrincipal, keytabURI),
                              e);
      }

      return new UGIWithPrincipal(impersonationRequest.getPrincipal(), loggedInUGI);
    } finally {
      if (!isKeytabLocal && !localKeytabFile.delete()) {
        LOG.warn("Failed to delete file: {}", localKeytabFile);
      }
    }
  }

  /**
   * Returns the path to a keytab file, after copying it to the local file system, if necessary
   *
   * @param keytabLocation the keytabLocation of the keytab file
   * @return local keytab file
   */
  private File localizeKeytab(Location keytabLocation) throws IOException {
    // ensure temp dir exists
    if (!DirUtils.mkdirs(tempDir)) {
      throw new IOException(String.format(
        "Could not create temporary directory at %s, while localizing keytab", tempDir));
    }

    // create a local file with restricted permissions
    // only allow the owner to read/write, since it contains credentials
    Path localKeytabFile = Files.createTempFile(tempDir.toPath(), null, "keytab.localized", FileUtils.OWNER_ONLY_RW);
    // copy to this local file
    LOG.debug("Copying keytab file from {} to {}", keytabLocation, localKeytabFile);
    try (InputStream is = keytabLocation.getInputStream()) {
      Files.copy(is, localKeytabFile, StandardCopyOption.REPLACE_EXISTING);
    }

    return localKeytabFile.toFile();
  }
  
  private Map<String, String> getRuntimeImpersonationProperties(NamespacedEntityId entityId) {    
    if (entityId == null) {
      LOG.debug("Entity Id null, skip fetching runtime args");
      return Collections.emptyMap();
    }
    
    if (!(entityId instanceof ProgramRunId) && (!(entityId instanceof TwillRunProgramId))) {
      LOG.debug("Entity Id not of type ProgramRunId or TwillRunProgramId, skip fetching runtime args");
      return Collections.emptyMap();
    }
      
    ProgramRunId programRunId;
    
    if (entityId instanceof TwillRunProgramId) {
      programRunId = getProgramRunIdFromTwillRunProgramId(entityId);
      if (programRunId == null) {
        return Collections.emptyMap();
      }
    } else {
      programRunId = ((ProgramRunId) entityId);
    }
          
    RunRecordMeta runRecord = null;
    long sleepDelayMs = TimeUnit.MILLISECONDS.toMillis(100);
    long timeoutMs = TimeUnit.SECONDS.toMillis(10);

    try {
      runRecord = Retries.callWithRetries(() -> {
        RunRecordMeta rec = store.getRun(programRunId);
        if (rec != null) {
          return rec;
        }
        throw new Exception("Retrying again to fetch run record");
      }, RetryStrategies.timeLimit(timeoutMs, TimeUnit.MILLISECONDS,
                  RetryStrategies.fixDelay(sleepDelayMs, TimeUnit.MILLISECONDS)),
                  Exception.class::isInstance);
    } catch (Exception e) {
      LOG.warn("Failed to fetch run record for {} due to {}", programRunId, e.getMessage(), e);
      return Collections.emptyMap();
    }

    Map<String, String> resultProps = Collections.emptyMap();
    Type stringStringMap = new TypeToken<Map<String, String>>() { }.getType();
    Map<String, String> properties = runRecord.getProperties();
    LOG.debug("Runtime args for program {} are sysargs {}, properties {}", programRunId,
            Arrays.toString(runRecord.getSystemArgs().entrySet().toArray()),
            Arrays.toString(properties.entrySet().toArray()));
    String runtimeArgsJson = properties.get("runtimeArgs");
    if (runtimeArgsJson != null) {
      properties = gson.fromJson(runtimeArgsJson, stringStringMap);
      // To avoid any conflicts with user supplied runtime arguments, if same named property
      // is provided by user as user runtime arguments, remove it from our map
      if (properties.containsKey(ProgramOptionConstants.LOGGED_IN_USER)) {
        properties.remove(ProgramOptionConstants.LOGGED_IN_USER);
      }
      resultProps = properties;
      resultProps.putAll(runRecord.getSystemArgs());
    } else {
      LOG.debug("Could not find any runtime args for program {}", programRunId);
      resultProps = runRecord.getSystemArgs();
    }
    return resultProps;
  }
  
  private ProgramRunId getProgramRunIdFromTwillRunProgramId(NamespacedEntityId entityId) {
    if (!(entityId instanceof TwillRunProgramId)) {
      LOG.debug("Enity {} not of type Twill ProgramId", entityId);
      return null;
    }
    
    TwillRunProgramId twillProgramId = ((TwillRunProgramId) entityId);
    if (twillProgramId.getTwillRunId() == null) {
      LOG.debug("Enity {} has twill run id as null", entityId);
      return null;
    }

    Map<RunId, RuntimeInfo> runInfos = this.runtimeService.list(twillProgramId.getProgramId());
    if (runInfos == null) {
      LOG.debug("Could not find any runtime infos for {}", twillProgramId);
      return null;
    }
    
    for (RunId key : runInfos.keySet()) {
      RuntimeInfo runInfo = runInfos.get(key);
      String twillId = runInfo.getTwillRunId().getId();
      if ((twillId != null) && (twillId.equals(twillProgramId.getTwillRunId()))) {
        LOG.debug("Found RunId {} for Twill ProgramId {}", key.getId(), twillProgramId);
        return new ProgramRunId(twillProgramId.getNamespace(),
              twillProgramId.getProgramId().getApplication(),
              twillProgramId.getProgramId().getType(),
              twillProgramId.getProgramId().getProgram(),
              key.getId());
      }
    }
    LOG.debug("No ProgramRunId found matching Twill ProgramId {}", twillProgramId);  
    return null;
  }

  private UserGroupInformation getProxyUGI(UserGroupInformation currentUgi, NamespacedEntityId entityId, 
          String loggedInUser) {
    UserGroupInformation proxyUgi = currentUgi;
    
    if (loggedInUser == null) {
      LOG.debug("Logged in user set as null");
      return currentUgi;
    }
    
    if (loggedInUser.equals(currentUgi.getShortUserName())) {
      LOG.debug("Current UGI {} already set as loggedInUser {}, skipping creation of proxy user",
              currentUgi, loggedInUser);
      return currentUgi;
    }
    
    ProgramId programId = null;
    if (entityId instanceof TwillRunProgramId) {
      programId = ((TwillRunProgramId)entityId).getProgramId();
    } else if (entityId instanceof ProgramRunId) {
      programId = ((ProgramRunId) entityId).getParent();
    }

    if (programId != null) {
      String pCategory = programId.getType().getCategoryName();
      if (!(pCategory.equalsIgnoreCase(ProgramType.SERVICE.getCategoryName()))) {
        proxyUgi = UserGroupInformation.createProxyUser(loggedInUser, currentUgi);
      }
    }

    return proxyUgi;
  }
  
  @Override
  protected String getImpersonatedUser(ImpersonationRequest impersonationRequest) {
    Map<String, String> properties = getRuntimeImpersonationProperties(impersonationRequest.getEntityId());

    String defaultRes;
    try {
      defaultRes = new KerberosName(impersonationRequest.getPrincipal()).getShortName();
    } catch (IOException e) {
        LOG.warn("Exception while converting principal {} to short name: {}",
                impersonationRequest.getPrincipal(), e);
        defaultRes = impersonationRequest.getPrincipal();
    }
    
    if (properties == null) {
      return defaultRes;
    }

    if (properties.containsKey(ProgramOptionConstants.LOGGED_IN_USER) && 
            (properties.containsKey(SystemArguments.USER_IMPERSONATION_ENABLED))) {
      return properties.get(ProgramOptionConstants.LOGGED_IN_USER);
    }

    if ((properties.containsKey(RUNTIME_ARG_KEYTAB)) &&
      (properties.containsKey(RUNTIME_ARG_PRINCIPAL))) {
      String principal = properties.get(RUNTIME_ARG_PRINCIPAL);
      try {
        return new KerberosName(principal).getShortName();
      } catch (IOException e) {
        LOG.warn("Exception while converting principal {} to short name: {}", principal, e);
        return principal;
      }  
    }
    return defaultRes;
  }
}
