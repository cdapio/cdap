package io.cdap.cdap.common.app;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.ApplicationConfigUpdateAction;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.app.ApplicationValidationContext;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.ArtifactVersionRange;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.common.PluginNotExistsException;
import io.cdap.cdap.common.id.Id.Artifact;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDescriptor;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultApplicationValidationContext implements ApplicationValidationContext {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultApplicationValidationContext.class);
  private static final Gson GSON = new GsonBuilder().
      registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory()).create();

  private final ArtifactId applicationArtifactId;
  private final String configString;
  private final ReadonlyArtifactRepositoryAccessor artifactRepository;
  private final ApplicationId applicationId;
  private final NamespaceId namespaceId;
  private final Set<ArtifactScope> allowedArtifactScopes;
  private final boolean allowSnapshot;

  public DefaultApplicationValidationContext(NamespaceId namespaceId, ApplicationId applicationId,
      ArtifactId applicationArtifactId, ReadonlyArtifactRepositoryAccessor artifactRepository,
      String configString, Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot) {
    this.namespaceId = namespaceId;
    this.applicationId = applicationId;
    this.artifactRepository = artifactRepository;
    this.applicationArtifactId = applicationArtifactId;
    this.configString = configString;
    this.allowedArtifactScopes = Collections.unmodifiableSet(allowedArtifactScopes);
    this.allowSnapshot = allowSnapshot;
  }

  @Override
  public <C extends Config> C getConfig(Type configType) {
    // Given configtype has to be derived from Config class.
    Preconditions.checkArgument(
        Config.class.isAssignableFrom(TypeToken.of(configType).getRawType()),
        "Application config type " + configType + " is not supported. "
            + "Type must extend Config and cannot be parameterized.");
    if (configString.isEmpty()) {
      try {
        return ((Class<C>) TypeToken.of(configType).getRawType()).newInstance();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Issue in creating config class of type " + configType.getTypeName(), e);
      }
    }

    try {
      return GSON.fromJson(configString, configType);
    } catch (JsonSyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid JSON application configuration was provided. Please check the"
              + " syntax.", e);
    }
  }

  @Override
  public List<ArtifactId> getPluginArtifacts(String pluginType, String pluginName,
      @Nullable ArtifactVersionRange pluginRange, int limit) throws Exception {
    List<ArtifactId> candidates = new ArrayList<>();
    for (ArtifactScope scope : this.allowedArtifactScopes) {
      candidates.addAll(
          getScopedPluginArtifacts(pluginType, pluginName, scope, pluginRange, limit));
    }
    return candidates;
  }

  private List<ArtifactId> getScopedPluginArtifacts(String pluginType, String pluginName,
      ArtifactScope pluginScope,
      @Nullable ArtifactVersionRange pluginRange, int limit)
      throws Exception {
    List<ArtifactId> pluginArtifacts = new ArrayList<>();
    NamespaceId pluginArtifactNamespace =
        ArtifactScope.SYSTEM.equals(pluginScope) ? NamespaceId.SYSTEM : namespaceId;

    Predicate<io.cdap.cdap.proto.id.ArtifactId> predicate = input -> {
      // Check if it is from the scoped namespace and should check if plugin is in given range if provided.
      return (pluginArtifactNamespace.equals(input.getParent())
          && (pluginRange == null || pluginRange.versionIsInRange(
          new ArtifactVersion(input.getVersion()))));
    };

    try {
      Map<ArtifactDescriptor, PluginClass> plugins =
          artifactRepository.getPlugins(pluginArtifactNamespace,
              Artifact.from(Namespace.fromEntityId(namespaceId), applicationArtifactId),
              pluginType, pluginName, predicate, limit, ArtifactSortOrder.ASC);
      for (Map.Entry<ArtifactDescriptor, PluginClass> pluginsEntry : plugins.entrySet()) {
        ArtifactId plugin = pluginsEntry.getKey().getArtifactId();
        // Consider if it is a non-snapshot version artifact or it is a snapshot version than allowSnapshot is true.
        if ((plugin.getVersion().isSnapshot() && allowSnapshot) || !plugin.getVersion()
            .isSnapshot()) {
          pluginArtifacts.add(plugin);
        }
      }
    } catch (PluginNotExistsException e) {
      LOG.trace("No plugin found for plugin {} of type {} in scope {} for app {}",
          pluginName, pluginType, pluginScope, applicationId, e);
      return Collections.emptyList();
    } catch (Exception e) {
      throw e;
    }
    return pluginArtifacts;
  }
}
