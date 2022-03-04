/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.cdap.internal.provision;

import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.gateway.handlers.ConsoleSettingsHttpHandler;
import io.cdap.cdap.proto.provisioner.ProvisionerDetail;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSystemContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Loads provisioners from the extensions directory.
 */
public class ProvisionerExtensionLoader extends AbstractExtensionLoader<String, Provisioner>
    implements ProvisionerProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ProvisionerExtensionLoader.class);
  private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
  private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

  private final Injector injector;
  private final ProvisionerConfigProvider provisionerConfigProvider;
  private final AtomicReference<ProvisionerInfo> provisionerInfo;

  private static Set<String> createAllowedResources() {
    try {
      return ClassPathResources.getResourcesWithDependencies(Provisioner.class.getClassLoader(),
          Provisioner.class);
    } catch (IOException e) {
      throw new RuntimeException("Failed to trace dependencies for provisioner extension. " +
          "Usage of provisioner might fail.", e);
    }
  }

  @Inject
  ProvisionerExtensionLoader(Injector injector, CConfiguration cConf,
      ProvisionerConfigProvider provisionerConfigProvider) {
    super(cConf.get(Constants.Provisioner.EXTENSIONS_DIR));
    this.injector = injector;
    this.provisionerConfigProvider = provisionerConfigProvider;
    this.provisionerInfo = new AtomicReference<>(new ProvisionerInfo(new HashMap<>(), new HashMap<>()));
  }

  @Override
  protected Set<String> getSupportedTypesForProvider(Provisioner provisioner) {
    return Collections.singleton(provisioner.getSpec().getName());
  }

  @Override
  protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
    // filter classes to provide isolation from CDAP's classes. For example, dataproc provisioner uses
    // a different guava than CDAP's guava.
    return new FilterClassLoader.Filter() {
      @Override
      public boolean acceptResource(String resource) {
        return ALLOWED_RESOURCES.contains(resource);
      }

      @Override
      public boolean acceptPackage(String packageName) {
        return ALLOWED_PACKAGES.contains(packageName);
      }
    };
  }

  @Override
  protected Provisioner prepareSystemExtension(Provisioner provisioner) {
    injector.injectMembers(provisioner);
    return provisioner;
  }

  /**
   * Reloads provisioners in the extension directory. Any new provisioners will be added and any deleted provisioners
   * will be removed. Loaded provisioners will be initialized.
   */
  @Override
  public void initializeProvisioners(CConfiguration cConf) {
    Map<String, Provisioner> provisioners = loadProvisioners();
    Map<String, ProvisionerConfig> provisionerConfigs =
        provisionerConfigProvider.loadProvisionerConfigs(provisioners.values());
    LOG.debug("Provisioners = {}", provisioners);
    Map<String, ProvisionerDetail> details = new HashMap<>(provisioners.size());
    for (Map.Entry<String, Provisioner> provisionerEntry : provisioners.entrySet()) {
      String provisionerName = provisionerEntry.getKey();
      Provisioner provisioner = provisionerEntry.getValue();
      ProvisionerSystemContext provisionerSystemContext = new DefaultSystemProvisionerContext(cConf,
          provisionerName);
      try {
        provisioner.initialize(provisionerSystemContext);
      } catch (RuntimeException e) {
        LOG.warn("Error initializing the {} provisioner. It will not be available for use.",
            provisionerName, e);
        provisioners.remove(provisionerName);
        continue;
      }
      ProvisionerSpecification spec = provisioner.getSpec();
      ProvisionerConfig config = provisionerConfigs.getOrDefault(provisionerName,
          new ProvisionerConfig(new ArrayList<>(), null, null, false));
      details.put(provisionerName,
          new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
              config.getConfigurationGroups(), config.getFilters(),
              config.getIcon(), config.isBeta()));
    }
    provisionerInfo.set(new ProvisionerInfo(provisioners, details));
  }

  @Override
  public ProvisionerInfo getProvisionerInfo() {
    return provisionerInfo.get();
  }

  @Override
  public Map<String, Provisioner> loadProvisioners() {
    return getAll();
  }
}
