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

package co.cask.cdap.internal.app.runtime.adapter;

import co.cask.cdap.adapter.AdapterSpecification;
import co.cask.cdap.adapter.Sink;
import co.cask.cdap.adapter.Source;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.app.store.StoreFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.DatasetManagementException;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.schedule.Scheduler;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.quartz.DateBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import javax.annotation.Nullable;

/**
 * Utility service that provides access to adapterTypeInfos currently registered
 */
public class AdapterService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(AdapterService.class);
  private static final Gson GSON = new Gson();
  private static final Type STRING_STRING_MAP_TYPE = new TypeToken<Map<String, String>>(){}.getType();
  private static final String ADAPTER_SPEC = "adapter.spec";
  private static final String DATASET_CLASS = "dataset.class";

  private final CConfiguration configuration;
  private Map<String, AdapterTypeInfo> adapterTypeInfos;
  private final DatasetFramework datasetFramework;
  private final StreamAdmin streamAdmin;
  private final Scheduler scheduler;
  private final Store store;

  @Inject
  public AdapterService(CConfiguration configuration, DatasetFramework datasetFramework, Scheduler scheduler,
                        StreamAdmin streamAdmin, StoreFactory storeFactory) {
    this.configuration = configuration;
    this.datasetFramework = datasetFramework;
    this.scheduler = scheduler;
    this.streamAdmin = streamAdmin;
    this.store = storeFactory.create();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting AdapterInfoService");
    this.adapterTypeInfos = Maps.newHashMap();
    registerAdapters();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down AdapterInfoService");
  }

  /**
   * Get the {@link AdapterTypeInfo} for a given adapter type.
   *
   * @param adapterType adapter type
   * @return instance of {@link AdapterTypeInfo} if available, null otherwise
   */
  @Nullable
  public AdapterTypeInfo getAdapterTypeInfo(String adapterType) {
    return this.adapterTypeInfos.get(adapterType);
  }

  /**
   * Retrieves the {@link AdapterSpecification} specified by the name in a given namespace.
   *
   * @param namespace namespace to lookup the adapter
   * @param adapterName name of the adapter
   * @return requested {@link AdapterSpecification} or null if no such AdapterInfo exists
   */
  @Nullable
  public AdapterSpecification getAdapter(String namespace, String adapterName) {
    return store.getAdapter(Id.Namespace.from(namespace), adapterName);
  }

  public Collection<AdapterSpecification> getAdapters(String namespace) {
    return store.getAllAdapters(Id.Namespace.from(namespace));
  }

  public void createAdapter(String namespaceId, AdapterSpecification adapterSpec) throws IllegalArgumentException {

    AdapterTypeInfo adapterTypeInfo = adapterTypeInfos.get(adapterSpec.getType());
    Preconditions.checkNotNull(adapterTypeInfo, "Adapter type %s not found", adapterSpec.getType());

    ApplicationSpecification appSpec = store.getApplication(Id.Application.from(namespaceId, adapterSpec.getType()));
    Preconditions.checkNotNull(appSpec, "Application %s not found for the adapter %s",
                               adapterSpec.getType(), adapterSpec.getName());

    validateSources(adapterSpec.getName(), adapterSpec.getSources());
    createSinks(adapterSpec.getSinks(), adapterTypeInfo);

    // If the adapter already exists, remove existing schedule to replace with the new one.
    AdapterSpecification existingSpec = store.getAdapter(Id.Namespace.from(namespaceId), adapterSpec.getName());
    if (existingSpec != null) {
      // TODO: Remove the schedule.
    }

    startPrograms(appSpec, adapterTypeInfo, adapterSpec);
    store.addAdapter(Id.Namespace.from(namespaceId), adapterSpec);
  }

  // Start all the programs needed for the adapter. Currently, only scheduling of workflow is supported.
  private void startPrograms(ApplicationSpecification spec, AdapterTypeInfo adapterTypeInfo,
                             AdapterSpecification adapterSpec) {
    ProgramType programType = adapterTypeInfo.getProgramType();
    Map<String, String> adapterProperties = Maps.newHashMap();
    adapterProperties.putAll(adapterTypeInfo.getDefaultAdapterProperties());
    adapterProperties.putAll(adapterSpec.getProperties());
    if (programType.equals(ProgramType.WORKFLOW)) {
      Map<String, WorkflowSpecification> workflowSpecs = spec.getWorkflows();
      for (Map.Entry<String, WorkflowSpecification> entry : workflowSpecs.entrySet()) {
        //TODO: Schedule all programs (passing the merged adapterProperties into the schedule spec's properties
      }
    } else {
      // Only Workflows are supported to be scheduled in the current implementation
      throw new UnsupportedOperationException(String.format("Unsupported program type %s for adapter",
                                                             programType.toString()));
    }

  }

  // Sources for all adapters should exists before creating the adapters.
  private void validateSources(String adapterName, Set<Source> sources) throws IllegalArgumentException {
    // Ensure all sources exist
    for (Source source : sources) {
      if (Source.Type.STREAM.equals(source.getType())) {
        if (!streamExists(source.getName())) {
          throw new IllegalArgumentException(String.format("Stream %s must exist during create of adapter: %s",
                                                           source.getName(), adapterName));
        }
      } else {
        throw new IllegalArgumentException(String.format("Unknown Source type: %s", source.getType()));
      }
    }
  }

  private boolean streamExists(String streamName) {
    try {
      if (!streamAdmin.exists(streamName)) {
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // create the required sinks for the adapters. Currently only DATASET sink type is supported.
  private void createSinks(Set<Sink> sinks, AdapterTypeInfo adapterTypeInfo) {
    // create sinks if not exist
    for (Sink sink : sinks) {
      if (Sink.Type.DATASET.equals(sink.getType())) {
        String datasetName = sink.getName();
        // add all propeties that were defined in the manifest (default sink properties), override that with sink
        // properties passed while creating the sinks.
        DatasetProperties properties = DatasetProperties.builder()
                                            .addAll(adapterTypeInfo.getDefaultSinkProperties())
                                            .addAll(sink.getProperties())
                                            .build();
        createDataset(datasetName, properties.getProperties().get(DATASET_CLASS), properties);
      } else {
        throw new IllegalArgumentException(String.format("Unknown Sink type: %s", sink.getType()));
      }
    }
  }

  private void createDataset(String datasetName, String datasetClass, DatasetProperties properties) {
    Preconditions.checkNotNull(datasetClass, "Dataset class cannot be null");
    try {
      if (!datasetFramework.hasInstance(datasetName)) {
        datasetFramework.addInstance(datasetClass, datasetName, properties);
      } else {
        LOG.debug("Dataset instance {} already exists not creating a new one.", datasetName);
      }
    } catch (DatasetManagementException e) {
      LOG.error("Error while creating dataset {}", datasetName, e);
      throw new RuntimeException(e);
    } catch (IOException e) {
      LOG.error("Error while creating dataset {}", datasetName, e);
      throw new RuntimeException(e);
    }
  }

  // Reads all the jars from the adapter directory and sets up required internal structures.
  @VisibleForTesting
  void registerAdapters() {
    try {
      File baseDir = new File(configuration.get(Constants.AppFabric.ADAPTER_DIR));
      Collection<File> files = FileUtils.listFiles(baseDir, new String[]{"jar"}, true);
      for (File file : files) {
        try {
          Manifest manifest = new JarFile(file.getAbsolutePath()).getManifest();
          AdapterTypeInfo adapterTypeInfo = createAdapterTypeInfo(file, manifest);
          if (adapterTypeInfo != null) {
            adapterTypeInfos.put(adapterTypeInfo.getType(), adapterTypeInfo);
          } else {
            LOG.error("Missing required information to create adapter {}", file.getAbsolutePath());
          }
        } catch (IOException e) {
          LOG.warn(String.format("Unable to read adapter jar %s", file.getAbsolutePath()));
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to read the plugins directory");
    }
  }


  private AdapterTypeInfo createAdapterTypeInfo(File file, Manifest manifest) {
    if (manifest != null) {
      Attributes mainAttributes = manifest.getMainAttributes();

      String adapterType = mainAttributes.getValue("CDAP-Adapter-Type");
      String sourceType = mainAttributes.getValue("CDAP-Source-Type");
      String sinkType = mainAttributes.getValue("CDAP-Sink-Type");
      String defaultSourceProperties = mainAttributes.getValue("CDAP-Source-Properties");
      String defaultSinkProperties = mainAttributes.getValue("CDAP-Sink-Properties");
      String defaultAdapterProperties = mainAttributes.getValue("CDAP-Adapter-Properties");
      String adapterProgramType = mainAttributes.getValue("CDAP-Adapter-Program-Type");

      if (adapterType != null && sourceType != null && sinkType != null && adapterProgramType != null) {
        return new AdapterTypeInfo(file, adapterType, Source.Type.valueOf(sourceType.toUpperCase()),
                                   Sink.Type.valueOf(sinkType.toUpperCase()),
                                   propertiesFromString(defaultSourceProperties),
                                   propertiesFromString(defaultSinkProperties),
                                   propertiesFromString(defaultAdapterProperties),
                                   ProgramType.valueOf(adapterProgramType.toUpperCase()));
      }
    }
    return null;
  }

  protected Map<String, String> propertiesFromString(String gsonEncodedMap) {
    Map<String, String> properties =  GSON.fromJson(gsonEncodedMap, STRING_STRING_MAP_TYPE);
    return properties == null ? Maps.<String, String>newHashMap() : properties;
  }

  /**
   * Holds information about an Adapter
   */
  public static final class AdapterTypeInfo {

    private final File file;
    private final String type;
    private final Source.Type sourceType;
    private final Sink.Type sinkType;
    private final Map<String, String> defaultSourceProperties;
    private final Map<String, String> defaultSinkProperties;
    private final Map<String, String> defaultAdapterProperties;
    private final ProgramType programType;

    public AdapterTypeInfo(File file, String adapterType, Source.Type sourceType, Sink.Type sinkType,
                           Map<String, String> defaultSourceProperties,
                           Map<String, String> defaultSinkProperties,
                           Map<String, String> defaultAdapterProperties,
                           ProgramType programType) {
      this.file = file;
      this.type = adapterType;
      this.sourceType = sourceType;
      this.sinkType = sinkType;
      this.defaultSourceProperties = ImmutableMap.copyOf(defaultSourceProperties);
      this.defaultSinkProperties = ImmutableMap.copyOf(defaultSinkProperties);
      this.defaultAdapterProperties = ImmutableMap.copyOf(defaultAdapterProperties);
      this.programType = programType;
    }

    public File getFile() {
      return file;
    }

    public String getType() {
      return type;
    }

    public Source.Type getSourceType() {
      return sourceType;
    }

    public Sink.Type getSinkType() {
      return sinkType;
    }

    public Map<String, String> getDefaultSourceProperties() {
      return defaultSourceProperties;
    }

    public Map<String, String> getDefaultSinkProperties() {
      return defaultSinkProperties;
    }

    public Map<String, String> getDefaultAdapterProperties() {
      return defaultAdapterProperties;
    }

    public ProgramType getProgramType() {
      return programType;
    }
  }

  /**
   * Converts a frequency expression into cronExpression that is usable by quartz.
   * Supports frequency expressions with the following resolutions: minutes, hours, days.
   * Example conversions:
   * '10m' -> '*{@literal /}10 * * * ?'
   * '3d' -> '0 0 *{@literal /}3 * ?'
   *
   * @return a cron expression
   */
  // TODO: package private?
  @VisibleForTesting
  public static String toCronExpr(String frequency) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(frequency));
    // remove all whitespace
    frequency = frequency.replaceAll("\\s+", "");
    Preconditions.checkArgument(frequency.length() >= 0);

    frequency = frequency.toLowerCase();

    String value = frequency.substring(0, frequency.length() - 1);
    Preconditions.checkArgument(StringUtils.isNumeric(value));
    Integer parsedValue = Integer.valueOf(value);
    Preconditions.checkArgument(parsedValue > 0);

    String everyN = String.format("*/%s", value);
    char lastChar = frequency.charAt(frequency.length() - 1);
    switch (lastChar) {
      case 'm':
        DateBuilder.validateMinute(parsedValue);
        return String.format("%s * * * ?", everyN);
      case 'h':
        DateBuilder.validateHour(parsedValue);
        return String.format("0 %s * * ?", everyN);
      case 'd':
        DateBuilder.validateDayOfMonth(parsedValue);
        return String.format("0 0 %s * ?", everyN);
    }
    throw new IllegalArgumentException(String.format("Time unit not supported: %s", lastChar));
  }
}
