/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
package co.cask.cdap.data2.transaction.stream;

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.StreamNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.security.ImpersonationUtils;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.common.utils.OSDetector;
import co.cask.cdap.data.stream.CoordinatorStreamProperties;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamFileOffset;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.service.StreamMetaStore;
import co.cask.cdap.data.view.ViewAdmin;
import co.cask.cdap.data2.audit.AuditPublisher;
import co.cask.cdap.data2.audit.AuditPublishers;
import co.cask.cdap.data2.metadata.lineage.AccessType;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.metadata.system.StreamSystemMetadataWriter;
import co.cask.cdap.data2.metadata.system.SystemMetadataWriter;
import co.cask.cdap.data2.metadata.writer.LineageWriter;
import co.cask.cdap.data2.registry.RuntimeUsageRegistry;
import co.cask.cdap.explore.client.ExploreFacade;
import co.cask.cdap.explore.utils.ExploreTableNaming;
import co.cask.cdap.internal.io.SchemaTypeAdapter;
import co.cask.cdap.notifications.feeds.NotificationFeedException;
import co.cask.cdap.notifications.feeds.NotificationFeedManager;
import co.cask.cdap.proto.StreamProperties;
import co.cask.cdap.proto.ViewSpecification;
import co.cask.cdap.proto.audit.AuditPayload;
import co.cask.cdap.proto.audit.AuditType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NotificationFeedId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import co.cask.cdap.proto.notification.NotificationFeedInfo;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import co.cask.cdap.security.spi.authorization.UnauthorizedException;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

/**
 * An abstract base {@link StreamAdmin} for File based stream.
 */
public class FileStreamAdmin implements StreamAdmin {

  private static final String CONFIG_FILE_NAME = "config.json";

  private static final Logger LOG = LoggerFactory.getLogger(FileStreamAdmin.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private final NamespacedLocationFactory namespacedLocationFactory;
  private final StreamCoordinatorClient streamCoordinatorClient;
  private final CConfiguration cConf;
  private final StreamConsumerStateStoreFactory stateStoreFactory;
  private final NotificationFeedManager notificationFeedManager;
  private final String streamBaseDirPath;
  private final RuntimeUsageRegistry runtimeUsageRegistry;
  private final LineageWriter lineageWriter;
  private final StreamMetaStore streamMetaStore;
  private final ExploreTableNaming tableNaming;
  private final ViewAdmin viewAdmin;
  private final MetadataStore metadataStore;
  private final Impersonator impersonator;
  private final PrivilegesManager privilegesManager;
  private final AuthorizationEnforcer authorizationEnforcer;
  private final AuthenticationContext authenticationContext;

  private ExploreFacade exploreFacade;
  private AuditPublisher auditPublisher;

  @Inject
  public FileStreamAdmin(NamespacedLocationFactory namespacedLocationFactory,
                         CConfiguration cConf,
                         StreamCoordinatorClient streamCoordinatorClient,
                         StreamConsumerStateStoreFactory stateStoreFactory,
                         NotificationFeedManager notificationFeedManager,
                         RuntimeUsageRegistry runtimeUsageRegistry,
                         LineageWriter lineageWriter,
                         StreamMetaStore streamMetaStore,
                         ExploreTableNaming tableNaming,
                         MetadataStore metadataStore,
                         ViewAdmin viewAdmin,
                         Impersonator impersonator,
                         PrivilegesManager privilegesManager,
                         AuthenticationContext authenticationContext,
                         AuthorizationEnforcer authorizationEnforcer) {
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.cConf = cConf;
    this.notificationFeedManager = notificationFeedManager;
    this.privilegesManager = privilegesManager;
    this.streamBaseDirPath = cConf.get(Constants.Stream.BASE_DIR);
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.stateStoreFactory = stateStoreFactory;
    this.runtimeUsageRegistry = runtimeUsageRegistry;
    this.lineageWriter = lineageWriter;
    this.streamMetaStore = streamMetaStore;
    this.tableNaming = tableNaming;
    this.metadataStore = metadataStore;
    this.viewAdmin = viewAdmin;
    this.impersonator = impersonator;
    this.authenticationContext = authenticationContext;
    this.authorizationEnforcer = authorizationEnforcer;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setExploreFacade(ExploreFacade exploreFacade) {
    // Optional injection is used to simplify Guice injection since ExploreFacade is only need when explore is enabled
    this.exploreFacade = exploreFacade;
  }

  @SuppressWarnings("unused")
  @Inject(optional = true)
  public void setAuditPublisher(AuditPublisher auditPublisher) {
    this.auditPublisher = auditPublisher;
  }

  @Override
  public void dropAllInNamespace(final NamespaceId namespace) throws Exception {
    // To delete all streams in a namespace, one should have admin privileges on that namespace
    ensureAccess(namespace, Action.ADMIN);
    UserGroupInformation ugi = impersonator.getUGI(namespace);
    Iterable<Location> locations = ImpersonationUtils.doAs(ugi, new Callable<Iterable<Location>>() {
      @Override
      public Iterable<Location> call() throws Exception {
        try {
          return StreamUtils.listAllStreams(getStreamBaseLocation(namespace));
        } catch (FileNotFoundException e) {
          // If the stream base doesn't exists, nothing need to be deleted
          return ImmutableList.of();
        }
      }
    });

    for (final Location location : locations) {
      doDrop(namespace.stream(StreamUtils.getStreamNameFromLocation(location)), location, ugi);
    }

    // Also drop the state table
    ImpersonationUtils.doAs(ugi, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        stateStoreFactory.dropAllInNamespace(namespace);
        return null;
      }
    });
  }

  @Override
  public void configureInstances(StreamId streamId, long groupId, int instances) throws Exception {
    Preconditions.checkArgument(instances > 0, "Number of consumer instances must be > 0.");

    LOG.info("Configure instances: {} {}", groupId, instances);

    StreamConfig config = StreamUtils.ensureExists(this, streamId);
    try (StreamConsumerStateStore stateStore = stateStoreFactory.create(config)) {
      Set<StreamConsumerState> states = Sets.newHashSet();
      stateStore.getByGroup(groupId, states);

      Set<StreamConsumerState> newStates = Sets.newHashSet();
      Set<StreamConsumerState> removeStates = Sets.newHashSet();
      mutateStates(groupId, instances, states, newStates, removeStates);

      // Save the states back
      if (!newStates.isEmpty()) {
        stateStore.save(newStates);
        LOG.info("Configure instances new states: {} {} {}", groupId, instances, newStates);
      }
      if (!removeStates.isEmpty()) {
        stateStore.remove(removeStates);
        LOG.info("Configure instances remove states: {} {} {}", groupId, instances, removeStates);
      }
    }
  }

  @Override
  public void configureGroups(StreamId streamId, Map<Long, Integer> groupInfo) throws Exception {
    Preconditions.checkArgument(!groupInfo.isEmpty(), "Consumer group information must not be empty.");

    LOG.info("Configure groups for {}: {}", streamId, groupInfo);

    StreamConfig config = StreamUtils.ensureExists(this, streamId);
    try (StreamConsumerStateStore stateStore = stateStoreFactory.create(config)) {
      Set<StreamConsumerState> states = Sets.newHashSet();
      stateStore.getAll(states);

      // Remove all groups that are no longer exists. The offset information in that group can be discarded.
      Set<StreamConsumerState> removeStates = Sets.newHashSet();
      for (StreamConsumerState state : states) {
        if (!groupInfo.containsKey(state.getGroupId())) {
          removeStates.add(state);
        }
      }

      // For each groups, compute the new file offsets if needed
      Set<StreamConsumerState> newStates = Sets.newHashSet();
      for (Map.Entry<Long, Integer> entry : groupInfo.entrySet()) {
        final long groupId = entry.getKey();

        // Create a view of old states which match with the current groupId only.
        mutateStates(groupId, entry.getValue(), Sets.filter(
          states, new com.google.common.base.Predicate<StreamConsumerState>() {
          @Override
          public boolean apply(StreamConsumerState state) {
            return state.getGroupId() == groupId;
          }
        }), newStates, removeStates);
      }

      // Save the states back
      if (!newStates.isEmpty()) {
        stateStore.save(newStates);
        LOG.info("Configure groups new states: {} {}", groupInfo, newStates);
      }
      if (!removeStates.isEmpty()) {
        stateStore.remove(removeStates);
        LOG.info("Configure groups remove states: {} {}", groupInfo, removeStates);
      }
    }
  }

  @Override
  public void upgrade() throws Exception {
    // No-op
  }

  @Override
  public List<StreamSpecification> listStreams(final NamespaceId namespaceId) throws Exception {
    final Predicate<EntityId> filter = authorizationEnforcer.createFilter(authenticationContext.getPrincipal());
    List<StreamSpecification> streamSpecifications = streamMetaStore.listStreams(namespaceId);
    return Lists.newArrayList(Iterables.filter(streamSpecifications,
                                               new com.google.common.base.Predicate<StreamSpecification>() {
      @Override
      public boolean apply(StreamSpecification spec) {
        return filter.apply(namespaceId.stream(spec.getName()));
      }
    }));
  }

  @Override
  public StreamConfig getConfig(final StreamId streamId) throws IOException {
    // No Authorization check performed in this method. If required, it should be added before this method is invoked
    UserGroupInformation ugi = impersonator.getUGI(streamId.getParent());
    try {
      return ImpersonationUtils.doAs(ugi, new Callable<StreamConfig>() {
        @Override
        public StreamConfig call() throws IOException {
          Location configLocation = getConfigLocation(streamId);
          if (!configLocation.exists()) {
            throw new FileNotFoundException(String.format("Configuration file %s for stream '%s' does not exist.",
                                                          configLocation.toURI().getPath(), streamId));
          }
          StreamConfig config = GSON.fromJson(
            CharStreams.toString(CharStreams.newReaderSupplier(Locations.newInputSupplier(configLocation),
                                                               Charsets.UTF_8)), StreamConfig.class);

          int threshold = config.getNotificationThresholdMB();
          if (threshold <= 0) {
            // Need to default it for existing configs that were created before notification threshold was added.
            threshold = cConf.getInt(Constants.Stream.NOTIFICATION_THRESHOLD);
          }

          return new StreamConfig(streamId, config.getPartitionDuration(), config.getIndexInterval(),
                                  config.getTTL(), getStreamLocation(streamId), config.getFormat(), threshold);
        }
      });
    } catch (Exception ex) {
      Throwables.propagateIfPossible(ex, IOException.class);
      throw new IOException(ex);
    }
  }

  @Override
  public StreamProperties getProperties(StreamId streamId) throws Exception {
    // User should have any access on the stream to read its properties
    ensureAccess(streamId);
    StreamConfig config = getConfig(streamId);
    StreamSpecification spec = streamMetaStore.getStream(streamId);
    return new StreamProperties(config.getTTL(), config.getFormat(), config.getNotificationThresholdMB(),
                                spec.getDescription());
  }

  @Override
  public void updateConfig(final StreamId streamId, final StreamProperties properties) throws Exception {
    Location streamLocation;
    // User should have admin access on the stream to update its configuration
    ensureAccess(streamId, Action.ADMIN);
    streamLocation = impersonator.doAs(streamId.getParent(), new Callable<Location>() {
      @Override
      public Location call() throws Exception {
        return getStreamLocation(streamId);
      }
    });

    Preconditions.checkArgument(streamLocation.isDirectory(), "Stream '%s' does not exist.", streamId);
    streamCoordinatorClient.updateProperties(
      streamId, new Callable<CoordinatorStreamProperties>() {
        @Override
        public CoordinatorStreamProperties call() throws Exception {
          StreamProperties oldProperties = updateProperties(streamId, properties);

          FormatSpecification format = properties.getFormat();
          if (format != null) {
            // if the schema has changed, we need to recreate the hive table.
            // Changes in format and settings don't require
            // a hive change, as they are just properties used by the stream storage handler.
            Schema currSchema = oldProperties.getFormat().getSchema();
            Schema newSchema = format.getSchema();
            if (!currSchema.equals(newSchema)) {
              alterExploreStream(streamId, false, null);
              alterExploreStream(streamId, true, format);
            }
          }

          publishAudit(streamId, AuditType.UPDATE);
          return new CoordinatorStreamProperties(properties.getTTL(), properties.getFormat(),
                                                 properties.getNotificationThresholdMB(), null,
                                                 properties.getDescription());
        }
      });
  }

  @Override
  public boolean exists(final StreamId streamId) throws Exception {
    try {
      boolean metaExists = streamMetaStore.streamExists(streamId);
      if (!metaExists) {
        return false;
      }
      UserGroupInformation ugi = impersonator.getUGI(streamId.getParent());
      return ImpersonationUtils.doAs(ugi, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return getConfigLocation(streamId).exists();
        }
      });
    } catch (IOException e) {
      LOG.error("Exception when check for stream exist.", e);
      return false;
    }
  }

  @Override
  public StreamConfig create(StreamId streamId) throws Exception {
    return create(streamId, new Properties());
  }

  @Override
  public StreamConfig create(final StreamId streamId, @Nullable final Properties props) throws Exception {
    // User should have write access to the namespace
    NamespaceId streamNamespace = streamId.getParent();
    ensureAccess(streamNamespace, Action.WRITE);
    // revoke privleges to make sure there is no orphaned privleges
    privilegesManager.revoke(streamId);
    try {
      // Grant All access to the stream created to the User
      privilegesManager.grant(streamId, authenticationContext.getPrincipal(), EnumSet.allOf(Action.class));
      final UserGroupInformation ugi = impersonator.getUGI(streamNamespace);
      final Location streamLocation = ImpersonationUtils.doAs(ugi, new Callable<Location>() {
        @Override
        public Location call() throws Exception {
          assertNamespaceHomeExists(streamId.getParent());
          Location streamLocation = getStreamLocation(streamId);
          Locations.mkdirsIfNotExists(streamLocation);
          return streamLocation;
        }
      });

      return streamCoordinatorClient.createStream(streamId, new Callable<StreamConfig>() {
        @Override
        public StreamConfig call() throws Exception {
          if (exists(streamId)) {
            return null;
          }

          long createTime = System.currentTimeMillis();
          Properties properties = (props == null) ? new Properties() : props;
          long partitionDuration = Long.parseLong(properties.getProperty(
            Constants.Stream.PARTITION_DURATION, cConf.get(Constants.Stream.PARTITION_DURATION)));
          long indexInterval = Long.parseLong(properties.getProperty(
            Constants.Stream.INDEX_INTERVAL, cConf.get(Constants.Stream.INDEX_INTERVAL)));
          long ttl = Long.parseLong(properties.getProperty(
            Constants.Stream.TTL, cConf.get(Constants.Stream.TTL)));
          int threshold = Integer.parseInt(properties.getProperty(
            Constants.Stream.NOTIFICATION_THRESHOLD, cConf.get(Constants.Stream.NOTIFICATION_THRESHOLD)));
          String description = properties.getProperty(Constants.Stream.DESCRIPTION);
          FormatSpecification formatSpec = null;
          if (properties.containsKey(Constants.Stream.FORMAT_SPECIFICATION)) {
            formatSpec = GSON.fromJson(properties.getProperty(Constants.Stream.FORMAT_SPECIFICATION),
                                       FormatSpecification.class);
          }

          final StreamConfig config = new StreamConfig(streamId, partitionDuration, indexInterval,
                                                       ttl, streamLocation, formatSpec, threshold);
          ImpersonationUtils.doAs(ugi, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              writeConfig(config);
              return null;
            }
          });

          createStreamFeeds(config);
          alterExploreStream(streamId, true, config.getFormat());
          streamMetaStore.addStream(streamId, description);
          publishAudit(streamId, AuditType.CREATE);
          SystemMetadataWriter systemMetadataWriter =
            new StreamSystemMetadataWriter(metadataStore, streamId, config, createTime, description);
          systemMetadataWriter.write();
          return config;
        }
      });
    } catch (Exception e) {
      // there was a problem creating the stream. so revoke privilege.
      privilegesManager.revoke(streamId);
      throw e;
    }
  }

  private void assertNamespaceHomeExists(NamespaceId namespaceId) throws IOException {
    Location namespaceHomeLocation = Locations.getParent(getStreamBaseLocation(namespaceId));
    Preconditions.checkArgument(namespaceHomeLocation != null && namespaceHomeLocation.exists(),
                                "Home directory %s for namespace %s not found", namespaceHomeLocation, namespaceId);
  }

  /**
   * Create the public {@link NotificationFeedId}s that concerns the stream with configuration {@code config}.
   *
   * @param config config of the stream to create feeds for
   */
  private void createStreamFeeds(StreamConfig config) {
    try {
      NotificationFeedInfo streamFeed = new NotificationFeedInfo(
        config.getStreamId().getNamespace(),
        Constants.Notification.Stream.STREAM_FEED_CATEGORY,
        String.format("%sSize", config.getStreamId().getEntityName()),
        String.format("Size updates feed for Stream %s every %dMB",
                      config.getStreamId(), config.getNotificationThresholdMB()));
      notificationFeedManager.createFeed(streamFeed);
    } catch (NotificationFeedException e) {
      LOG.error("Cannot create feed for Stream {}", config.getStreamId(), e);
    }
  }

  @Override
  public void truncate(final StreamId streamId) throws Exception {
    // User should have ADMIN access to truncate the stream
    ensureAccess(streamId, Action.ADMIN);
    impersonator.doAs(streamId.getParent(), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        doTruncate(streamId, getStreamLocation(streamId));
        return null;
      }
    });
  }

  @Override
  public void drop(final StreamId streamId) throws Exception {
    // User should have ADMIN access to drop the stream
    ensureAccess(streamId, Action.ADMIN);
    UserGroupInformation ugi = impersonator.getUGI(streamId.getParent());
    Location streamLocation = ImpersonationUtils.doAs(ugi, new Callable<Location>() {
      @Override
      public Location call() throws Exception {
        return getStreamLocation(streamId);
      }
    });
    doDrop(streamId, streamLocation, ugi);
  }

  @Override
  public boolean createOrUpdateView(final StreamViewId viewId, final ViewSpecification spec) throws Exception {
    final StreamId stream = viewId.getParent();
    return streamCoordinatorClient.exclusiveAction(
      stream, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          if (!exists(stream)) {
            throw new NotFoundException(stream);
          }
          return viewAdmin.createOrUpdate(viewId, spec);
        }
      });
  }

  @Override
  public void deleteView(final StreamViewId viewId) throws Exception {
    final StreamId stream = viewId.getParent();
    streamCoordinatorClient.exclusiveAction(
      stream, new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          if (!exists(stream)) {
            throw new StreamNotFoundException(stream);
          }
          viewAdmin.delete(viewId);
          return null;
        }
      });
  }

  @Override
  public List<StreamViewId> listViews(final StreamId streamId) throws Exception {
    if (!exists(streamId)) {
      throw new StreamNotFoundException(streamId);
    }
    return viewAdmin.list(streamId);
  }

  @Override
  public ViewSpecification getView(final StreamViewId viewId) throws Exception {
    final StreamId stream = viewId.getParent();
    if (!exists(stream)) {
      throw new StreamNotFoundException(stream);
    }
    return viewAdmin.get(viewId);
  }

  @Override
  public boolean viewExists(StreamViewId viewId) throws Exception {
    StreamId stream = viewId.getParent();
    if (!exists(stream)) {
      throw new StreamNotFoundException(stream);
    }
    return viewAdmin.exists(viewId);
  }

  @Override
  public void register(Iterable<? extends EntityId> owners, StreamId streamId) {
    runtimeUsageRegistry.registerAll(owners, streamId);
  }

  @Override
  public void addAccess(ProgramRunId run, StreamId streamId, AccessType accessType) {
    lineageWriter.addAccess(run, streamId, accessType);
    AuditPublishers.publishAccess(auditPublisher, streamId, accessType, run);
  }

  /**
   * Returns the location that points the config file for the given stream.
   */
  private Location getConfigLocation(StreamId streamId) throws IOException {
    return getStreamLocation(streamId).append(CONFIG_FILE_NAME);
  }

  /**
   * Returns the location for the given stream.
   */
  private Location getStreamLocation(StreamId streamId) throws IOException {
    return getStreamBaseLocation(streamId.getParent()).append(streamId.getEntityName());
  }

  /**
   * Returns the location for the given namespace that contains all streams belong to that namespace.
   */
  private Location getStreamBaseLocation(NamespaceId namespace) throws IOException {
    return namespacedLocationFactory.get(namespace.toId()).append(streamBaseDirPath);
  }

  private void doTruncate(final StreamId streamId, final Location streamLocation) throws Exception {
    streamCoordinatorClient.updateProperties(streamId, new Callable<CoordinatorStreamProperties>() {
      @Override
      public CoordinatorStreamProperties call() throws Exception {
        int newGeneration = StreamUtils.getGeneration(streamLocation) + 1;
        Locations.mkdirsIfNotExists(StreamUtils.createGenerationLocation(streamLocation, newGeneration));
        publishAudit(streamId, AuditType.TRUNCATE);
        return new CoordinatorStreamProperties(null, null, null, newGeneration, null);
      }
    });
  }

  private void doDrop(final StreamId streamId, final Location streamLocation,
                      final UserGroupInformation ugi) throws Exception {
    // Delete the stream config so that calls that try to access the stream will fail after this call returns.
    // The stream coordinator client will notify all clients that stream has been deleted.
    streamCoordinatorClient.deleteStream(streamId, new Runnable() {
      @Override
      public void run() {
        try {
          final Location configLocation = ImpersonationUtils.doAs(ugi, new Callable<Location>() {
            @Override
            public Location call() throws Exception {
              Location configLocation = getConfigLocation(streamId);
              return configLocation.exists() ? configLocation : null;
            }
          });

          if (configLocation == null) {
            return;
          }
          alterExploreStream(streamId.getParent().stream(StreamUtils.getStreamNameFromLocation(streamLocation)), false,
                                                         null);

          // Drop the associated views
          List<StreamViewId> views = viewAdmin.list(streamId);
          for (StreamViewId view : views) {
            viewAdmin.delete(view);
          }


          ImpersonationUtils.doAs(ugi, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              if (!configLocation.delete()) {
                LOG.debug("Could not delete stream config location {}", streamLocation);
              }

              // Move the stream directory to the deleted directory
              // The target directory has a timestamp appended to the stream name
              // It is for the case when a stream is created and deleted in a short period of time before
              // the stream janitor kicks in.
              Location deleted = StreamUtils.getDeletedLocation(getStreamBaseLocation(streamId.getParent()));
              Locations.mkdirsIfNotExists(deleted);
              streamLocation.renameTo(deleted.append(streamId.getEntityName() + System.currentTimeMillis()));
              return null;
            }
          });

          streamMetaStore.removeStream(streamId);
          metadataStore.removeMetadata(streamId);
          // revoke all privileges on the stream
          privilegesManager.revoke(streamId);
          publishAudit(streamId, AuditType.DELETE);
        } catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  private StreamProperties updateProperties(StreamId streamId, StreamProperties properties) throws Exception {
    StreamConfig config = getConfig(streamId);

    StreamConfig.Builder builder = StreamConfig.builder(config);
    if (properties.getTTL() != null) {
      builder.setTTL(properties.getTTL());
    }
    if (properties.getFormat() != null) {
      builder.setFormatSpec(properties.getFormat());
    }
    if (properties.getNotificationThresholdMB() != null) {
      builder.setNotificationThreshold(properties.getNotificationThresholdMB());
    }

    // update stream description
    String description = properties.getDescription();
    if (description != null) {
      streamMetaStore.addStream(streamId, description);
    }

    final StreamConfig newConfig = builder.build();
    UserGroupInformation ugi = impersonator.getUGI(streamId.getParent());
    ImpersonationUtils.doAs(ugi, new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        writeConfig(newConfig);
        return null;
      }
    });

    // Update system metadata for stream
    SystemMetadataWriter systemMetadataWriter = new StreamSystemMetadataWriter(
      metadataStore, streamId, newConfig, description);
    systemMetadataWriter.write();

    return new StreamProperties(config.getTTL(), config.getFormat(), config.getNotificationThresholdMB());
  }

  private void writeConfig(StreamConfig config) throws IOException {
    Location configLocation = config.getLocation().append(CONFIG_FILE_NAME);
    Location tmpConfigLocation = configLocation.getTempFile(null);

    CharStreams.write(GSON.toJson(config), CharStreams.newWriterSupplier(
      Locations.newOutputSupplier(tmpConfigLocation), Charsets.UTF_8));

    try {
      // Windows does not allow renaming if the destination file exists so we must delete the configLocation
      if (OSDetector.isWindows()) {
        configLocation.delete();
      }
      tmpConfigLocation.renameTo(getConfigLocation(config.getStreamId()));
    } finally {
      Locations.deleteQuietly(tmpConfigLocation);
    }
  }

  private void mutateStates(long groupId, int instances, Set<StreamConsumerState> states,
                            Set<StreamConsumerState> newStates, Set<StreamConsumerState> removeStates) {
    int oldInstances = states.size();
    if (oldInstances == instances) {
      // If number of instances doesn't changed, no need to mutate any states
      return;
    }

    // Collects smallest offsets across all existing consumers
    // Map from event file location to file offset.
    // Use tree map to maintain ordering consistency in the offsets.
    // Not required by any logic, just easier to look at when logged.
    Map<Location, StreamFileOffset> fileOffsets = Maps.newTreeMap(Locations.LOCATION_COMPARATOR);

    for (StreamConsumerState state : states) {
      for (StreamFileOffset fileOffset : state.getState()) {
        StreamFileOffset smallestOffset = fileOffsets.get(fileOffset.getEventLocation());
        if (smallestOffset == null || fileOffset.getOffset() < smallestOffset.getOffset()) {
          fileOffsets.put(fileOffset.getEventLocation(), new StreamFileOffset(fileOffset));
        }
      }
    }

    // Constructs smallest offsets
    Collection<StreamFileOffset> smallestOffsets = fileOffsets.values();

    // When group size changed, reset all existing instances states to have smallest files offsets constructed above.
    for (StreamConsumerState state : states) {
      if (state.getInstanceId() < instances) {
        // Only keep valid instances
        newStates.add(new StreamConsumerState(groupId, state.getInstanceId(), smallestOffsets));
      } else {
        removeStates.add(state);
      }
    }

    // For all new instances, set files offsets to smallest one constructed above.
    for (int i = oldInstances; i < instances; i++) {
      newStates.add(new StreamConsumerState(groupId, i, smallestOffsets));
    }
  }

  private void alterExploreStream(StreamId stream, boolean enable, @Nullable FormatSpecification format) {
    if (cConf.getBoolean(Constants.Explore.EXPLORE_ENABLED)) {
      // It shouldn't happen.
      Preconditions.checkNotNull(exploreFacade, "Explore enabled but no ExploreFacade instance is available");
      try {
        if (enable) {
          exploreFacade.enableExploreStream(stream.toId(), tableNaming.getTableName(stream), format);
        } else {
          exploreFacade.disableExploreStream(stream.toId(), tableNaming.getTableName(stream));
        }
      } catch (Exception e) {
        // at this time we want to still allow using stream even if it cannot be used for exploration
        String msg = String.format("Cannot alter exploration to %s for stream %s: %s", enable, stream, e.getMessage());
        LOG.error(msg, e);
      }
    }
  }

  private void publishAudit(StreamId stream, AuditType auditType) {
    AuditPublishers.publishAudit(auditPublisher, stream, auditType, AuditPayload.EMPTY_PAYLOAD);
  }

  private <T extends EntityId> void ensureAccess(T entityId, Action action) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    authorizationEnforcer.enforce(entityId, principal, action);
  }

  private <T extends EntityId> void ensureAccess(T entityId) throws Exception {
    Principal principal = authenticationContext.getPrincipal();
    Predicate<EntityId> filter = authorizationEnforcer.createFilter(principal);
    if (!filter.apply(entityId)) {
      throw new UnauthorizedException(principal, entityId);
    }
  }
}
