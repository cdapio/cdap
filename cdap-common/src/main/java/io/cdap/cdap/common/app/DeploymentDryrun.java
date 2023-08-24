package io.cdap.cdap.common.app;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.Config;
import io.cdap.cdap.api.app.Application;
import io.cdap.cdap.api.app.ApplicationUpdateResult;
import io.cdap.cdap.api.app.ApplicationValidationResult;
import io.cdap.cdap.api.artifact.ApplicationClass;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.InvalidArtifactException;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.proto.DeploymentDryrunResult;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.artifact.ArtifactSortOrder;
import io.cdap.cdap.proto.artifact.artifact.ArtifactDetail;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DeploymentDryrun {
  private static final Gson GSON =
      new GsonBuilder().registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory())
          .create();
  private AppRequest<?> appRequest;
  private ApplicationId appId;
  private ReadonlyArtifactRepositoryAccessor artifactRepository;

  public DeploymentDryrun(ApplicationId appId, AppRequest<?> appRequest, ReadonlyArtifactRepositoryAccessor artifactRepository) {
    this.appRequest = appRequest;
    this.appId = appId;
    this.artifactRepository = artifactRepository;
  }

  public DeploymentDryrunResult deploy() throws Exception {
    NamespaceId namespace = appId.getParent();
    String appName = appId.getApplication();
    String appVersion = appId.getVersion();
    ArtifactSummary summary = appRequest.getArtifact();

    Object config = appRequest.getConfig();
    String configString = config == null ? null :
        config instanceof String ? (String) config : GSON.toJson(config);

    ArtifactId currentArtifact = new ArtifactId(
        summary.getName(),
        new ArtifactVersion(summary.getVersion()),
        summary.getScope()
    );
    ArtifactSummary candidateArtifact = getLatestAppArtifact(appId, currentArtifact,
        new HashSet<>(Arrays.asList(ArtifactScope.SYSTEM, ArtifactScope.USER)),
        true);

    ArtifactVersion candidateArtifactVersion = new ArtifactVersion(candidateArtifact.getVersion());

    // Current artifact should not have higher version than candidate artifact.
    if (currentArtifact.getVersion().compareTo(candidateArtifactVersion) > 0) {
      String error = String.format(
          "The current artifact has a version higher %s than any existing artifact.",
          currentArtifact.getVersion());
      throw new InvalidArtifactException(error);
    }

    boolean shouldUpgradeApplicationArtifact =
        currentArtifact.getVersion().compareTo(candidateArtifactVersion) != 0;

    ArtifactId newArtifactId =
        new ArtifactId(candidateArtifact.getName(), candidateArtifactVersion,
            candidateArtifact.getScope());
    ArtifactScope newArtifactScope = newArtifactId.getScope();
    NamespaceId newArtifactNamespace =
        newArtifactScope == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM : namespace;
    Id.Artifact newArtifact = Id.Artifact.fromEntityId(
        newArtifactNamespace.artifact(newArtifactId.getName(), newArtifactId.getVersion().getVersion())
    );
    ArtifactDetail newArtifactDetail = artifactRepository.getArtifact(newArtifact);


    ApplicationClass appClass = Iterables.getFirst(newArtifactDetail.getMeta().getClasses().getApps(),
        null);
    if (appClass == null) {
      // This should never happen.
      throw new IllegalStateException(
          String.format("No application class found in artifact '%s' in namespace '%s'.",
              newArtifactDetail.getDescriptor().getArtifactId(), appId.getParent()));
    }

    io.cdap.cdap.proto.id.ArtifactId artifactId =
        toProtoArtifactId(appId.getParent(),
            newArtifactDetail.getDescriptor().getArtifactId());

    DefaultApplicationValidationContext validationContext =
        new DefaultApplicationValidationContext(appId.getParent(), appId,
            newArtifactDetail.getDescriptor().getArtifactId(),
            artifactRepository, configString,
            new HashSet<>(Arrays.asList(ArtifactScope.SYSTEM, ArtifactScope.USER)), true);

    try (CloseableClassLoader artifactClassLoader =
        artifactRepository.createArtifactClassLoader(newArtifactDetail.getDescriptor(), artifactId)) {
      Object appMain = artifactClassLoader.loadClass(appClass.getClassName()).newInstance();
      if (!(appMain instanceof Application)) {
        throw new IllegalStateException(
            String.format("Application main class is of invalid type: %s",
                appMain.getClass().getName()));
      }
      Application<?> app = (Application<?>) appMain;

      if (!app.isUpdateSupported()) {
        String errorMessage = String.format("Application %s does not support update.", appId);
        throw new UnsupportedOperationException(errorMessage);
      }

      ApplicationValidationResult validationResult = app.validateConfig(validationContext);

      return new DeploymentDryrunResult(
          shouldUpgradeApplicationArtifact,
          shouldUpgradeApplicationArtifact ? summary : null,
          shouldUpgradeApplicationArtifact ? new ArtifactSummary(
              newArtifact.getName(),
              newArtifact.getVersion().getVersion(),
              newArtifactScope
          ) : null,
          validationResult
      );
    }
  }

  private ArtifactSummary getLatestAppArtifact(ApplicationId appId,
      ArtifactId currentArtifactId,
      Set<ArtifactScope> allowedArtifactScopes,
      boolean allowSnapshot)
      throws Exception {

    List<ArtifactSummary> availableArtifacts = new ArrayList<>();
    // Find candidate artifacts from all scopes we need to consider.
    for (ArtifactScope scope : allowedArtifactScopes) {
      NamespaceId artifactNamespaceToConsider =
          ArtifactScope.SYSTEM.equals(scope) ? NamespaceId.SYSTEM : appId.getParent();
      List<ArtifactSummary> artifacts;
      try {
        artifacts = artifactRepository.getArtifactSummaries(artifactNamespaceToConsider,
            currentArtifactId.getName(),
            Integer.MAX_VALUE, ArtifactSortOrder.ASC);
      } catch (ArtifactNotFoundException e) {
        // This can happen if we are looking for candidate artifact in multiple namespace.
        continue;
      }
      for (ArtifactSummary artifactSummary : artifacts) {
        ArtifactVersion artifactVersion = new ArtifactVersion(artifactSummary.getVersion());
        // Consider if it is a non-snapshot version artifact or it is a snapshot version than allowSnapshot is true.
        if ((artifactVersion.isSnapshot() && allowSnapshot) || !artifactVersion.isSnapshot()) {
          availableArtifacts.add(artifactSummary);
        }
      }
    }

    // Find the artifact with latest version.
    Optional<ArtifactSummary> newArtifactCandidate = availableArtifacts.stream().max(
        Comparator.comparing(artifactSummary -> new ArtifactVersion(artifactSummary.getVersion())));

    io.cdap.cdap.proto.id.ArtifactId currentArtifact =
        new io.cdap.cdap.proto.id.ArtifactId(appId.getNamespace(), currentArtifactId.getName(),
            currentArtifactId.getVersion().getVersion());
    return newArtifactCandidate.orElseThrow(() -> new ArtifactNotFoundException(currentArtifact));
  }


  private io.cdap.cdap.proto.id.ArtifactId toProtoArtifactId(NamespaceId namespaceId,
      ArtifactId artifactId) {
    ArtifactScope scope = artifactId.getScope();
    NamespaceId artifactNamespace =
        scope == ArtifactScope.SYSTEM ? NamespaceId.SYSTEM : namespaceId;
    return artifactNamespace.artifact(artifactId.getName(), artifactId.getVersion().getVersion());
  }

}
