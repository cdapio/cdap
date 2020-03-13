package io.cdap.cdap.internal.app.runtime.artifact;

import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.id.NamespaceId;

import java.io.IOException;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import org.apache.twill.filesystem.Location;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import javax.annotation.Nullable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.api.artifact.ArtifactInfo;
import io.cdap.cdap.api.artifact.ArtifactRange;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.api.plugin.PluginClass;
import io.cdap.cdap.api.plugin.PluginSelector;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.common.ArtifactAlreadyExistsException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ArtifactRangeNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.conf.ArtifactConfig;
import io.cdap.cdap.common.conf.ArtifactConfigReader;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data2.metadata.system.ArtifactSystemMetadataWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataServiceClient;
import io.cdap.cdap.internal.app.runtime.plugin.PluginNotExistsException;
import io.cdap.cdap.internal.app.spark.SparkCompatReader;
import io.cdap.cdap.proto.artifact.ApplicationClassInfo;
import io.cdap.cdap.proto.artifact.ApplicationClassSummary;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.annotation.Nullable;


public class LocalArtifactRepositoryReader implements ArtifactRepositoryReader {
  private final ArtifactStore artifactStore;


  @VisibleForTesting
  @Inject
  public LocalArtifactRepositoryReader(ArtifactStore artifactStore) {
    this.artifactStore = artifactStore;
  }

  @Override
  public ArtifactDetail getArtifact(Id.Artifact artifactId) throws Exception {
    return artifactStore.getArtifact(artifactId);
  }

  @Override
  public Map.Entry<ArtifactDescriptor, PluginClass> findPlugin(
    NamespaceId namespace, Id.Artifact artifactId, String pluginType, String pluginName, PluginSelector selector)
    throws IOException, PluginNotExistsException, ArtifactNotFoundException {
    ArtifactRange artifactRange = new ArtifactRange(artifactId.getNamespace().getId(), artifactId.getName(),
                                                    new ArtifactVersion(artifactId.getVersion().getVersion()), true,
                                                    new ArtifactVersion(artifactId.getVersion().getVersion()), true);
    SortedMap<ArtifactDescriptor, PluginClass> pluginClasses = artifactStore.getPluginClasses(
      namespace, artifactRange, pluginType, pluginName, null, Integer.MAX_VALUE, ArtifactSortOrder.UNORDERED);

    SortedMap<ArtifactId, PluginClass> idToPluginClass = new TreeMap<>();
    SortedMap<ArtifactId, ArtifactDescriptor>  idToDescriptor = new TreeMap<>();

    for (Map.Entry<ArtifactDescriptor, PluginClass> pluginClassEntry : pluginClasses.entrySet()) {
      idToDescriptor.put(pluginClassEntry.getKey().getArtifactId(), pluginClassEntry.getKey());
      idToPluginClass.put(pluginClassEntry.getKey().getArtifactId(), pluginClassEntry.getValue());
    }
    Map.Entry<ArtifactId, PluginClass> selected = selector.select(idToPluginClass);
    if (selected == null) {
          throw new PluginNotExistsException(namespace, pluginType, pluginName);
    }

    ArtifactId selectedArtifactId = selected.getKey();
    return Maps.immutableEntry(idToDescriptor.get(selectedArtifactId), idToPluginClass.get(selectedArtifactId));
  }
}
