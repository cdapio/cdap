package co.cask.cdap.test.app;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.internal.app.deploy.Specifications;
import co.cask.cdap.internal.app.runtime.artifact.LocalPluginFinder;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinder;
import co.cask.cdap.internal.app.runtime.artifact.PluginFinderTestBase;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.artifacts.AppWithPlugin;
import co.cask.cdap.test.artifacts.plugins.ToStringPlugin;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import org.junit.Test;

public class TestSpec extends TestFrameworkTestBase {
  @Test
  public void test() throws Exception {
    ArtifactId artifactId = NamespaceId.DEFAULT.artifact("app-with-plugin", "1.0.0-SNAPSHOT");
    addAppArtifact(artifactId, AppWithPlugin.class);

    ArtifactId pluginArtifactId = NamespaceId.DEFAULT.artifact("test-plugin", "1.0.0-SNAPSHOT");
    addPluginArtifact(pluginArtifactId, artifactId, ToStringPlugin.class);

    ApplicationId appId = NamespaceId.DEFAULT.app("AppWithPlugin");
    AppRequest createRequest = new AppRequest(new ArtifactSummary(artifactId.getArtifact(), artifactId.getVersion()));

    ApplicationManager appManager = deployApplication(appId, createRequest);

    ApplicationSpecification from = Specifications.from(new AppWithPlugin());
    System.out.println(from);

  }
}
