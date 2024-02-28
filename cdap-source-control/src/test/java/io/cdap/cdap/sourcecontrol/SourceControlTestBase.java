/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.sourcecontrol;

import com.google.common.hash.Hashing;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.CredentialsProvider;
import org.eclipse.jgit.transport.RefSpec;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;


/**
 * Base class for source control tests.
 */
public abstract class SourceControlTestBase {

  protected static final String DEFAULT_BRANCH_NAME = "develop";
  protected static final String GIT_SERVER_USERNAME = "oauth2";
  protected static final int GIT_COMMAND_TIMEOUT = 2;
  protected static final String MOCK_TOKEN = UUID.randomUUID().toString();
  protected static final String NAMESPACE = "namespace1";
  protected static final String PASSWORD_NAME = "github-pat";
  protected static final PatConfig PAT_CONFIG = new PatConfig(PASSWORD_NAME, GIT_SERVER_USERNAME);
  protected static final AuthConfig AUTH_CONFIG = new AuthConfig(AuthType.PAT, PAT_CONFIG);
  protected static final String PATH_PREFIX = "pathPrefix";
  protected static final String TEST_APP_NAME = "app1";
  protected static final String TEST_APP2_NAME = "app2";
  protected static final String TEST_APP_SPEC = "{\n"
      + "  \"artifact\": {\n"
      + "     \"name\": \"cdap-notifiable-workflow\",\n"
      + "     \"version\": \"1.0.0\",\n"
      + "     \"scope\": \"system\"\n"
      + "  },\n"
      + "  \"config\": {\n"
      + "     \"plugin\": {\n"
      + "        \"name\": \"WordCount\",\n"
      + "        \"type\": \"sparkprogram\",\n"
      + "        \"artifact\": {\n"
      + "           \"name\": \"word-count-program\",\n"
      + "           \"scope\": \"user\",\n"
      + "           \"version\": \"1.0.0\"\n"
      + "        }\n"
      + "     }\n"
      + "  },\n"
      + "  \"preview\" : {\n"
      + "    \"programName\" : \"WordCount\",\n"
      + "    \"programType\" : \"spark\"\n"
      + "    },\n"
      + "  \"principal\" : \"test2\"\n"
      + "}";

  protected static final String TEST_FILE_HASH = "a163ca52d8b8180456f8233656ebefe5fe7a4851";

  @ClassRule
  public static TemporaryFolder baseTempFolder = new TemporaryFolder();


  public LocalGitServer getGitServer() {
    return new LocalGitServer(GIT_SERVER_USERNAME, MOCK_TOKEN, 0,
        DEFAULT_BRANCH_NAME,
        baseTempFolder);
  }

  /**
   * Clones the Git repository hosted by the {@link LocalGitServer}.
   *
   * @param dir the directory to clone the repository in.
   * @return the cloned {@link Git} object.
   */
  public Git getClonedGit(Path dir, LocalGitServer gitServer)
      throws GitAPIException {
    return Git.cloneRepository()
        .setURI(gitServer.getServerUrl() + "ignored")
        .setDirectory(dir.toFile())
        .setBranch(DEFAULT_BRANCH_NAME)
        .setTimeout(GIT_COMMAND_TIMEOUT)
        .setCredentialsProvider(
            new UsernamePasswordCredentialsProvider(GIT_SERVER_USERNAME,
                MOCK_TOKEN))
        .call();
  }

  /**
   * Adds a file to the git repository hosted by the {@link LocalGitServer}.
   *
   * @param relativePath path relative to git repo root.
   * @param contents     the contents of the file.
   * @return commit of the pushed change.
   */
  public RevCommit addFileToGit(Path relativePath, String contents,
      LocalGitServer gitServer)
      throws GitAPIException,
      IOException {
    Path tempDirPath = baseTempFolder.newFolder("temp-local-git").toPath();
    Git localGit = getClonedGit(tempDirPath, gitServer);
    Path absolutePath = tempDirPath.resolve(relativePath);
    // Create parent directories if they don't exist.
    Files.createDirectories(absolutePath.getParent());
    Files.write(absolutePath, contents.getBytes(StandardCharsets.UTF_8));
    localGit.add().addFilepattern(".").call();
    final RevCommit commit = localGit.commit()
        .setMessage("Adding " + relativePath)
        .call();
    localGit.push()
        .setTimeout(GIT_COMMAND_TIMEOUT)
        .setCredentialsProvider(getCredentialsProviderForLocalServer())
        .call();
    localGit.close();
    DirUtils.deleteDirectoryContents(tempDirPath.toFile());
    return commit;
  }

  /**
   * Calculates the hash for provided file contents in a similar way to Git.
   */
  public String getGitStyleHash(String fileContents) {
    // Git prefixes the object with "blob ", followed by the length (as a human-readable integer), followed by a NUL
    // character and takes the sha1 hash to find the file hash.
    // See https://stackoverflow.com/a/7225329.
    return Hashing.sha1()
        .hashString("blob " + fileContents.length() + "\0" + fileContents,
            StandardCharsets.UTF_8)
        .toString();
  }


  public void validatePullResponse(PullAppResponse<?> response, String appName) {
    Assert.assertEquals(response.getApplicationFileHash(), TEST_FILE_HASH);
    Assert.assertEquals(response.getApplicationName(), appName);
    AppRequest<?> appRequest = response.getAppRequest();
    Assert.assertNotNull(appRequest.getArtifact());
    Assert.assertEquals("cdap-notifiable-workflow",
        appRequest.getArtifact().getName());
    Assert.assertNotNull(appRequest.getPreview());
    Assert.assertEquals("WordCount", appRequest.getPreview().getProgramName());
  }

  /**
   * Deletes all the branches in the remote repository.
   *
   * @param gitServer the server hosting the remote repository.
   */
  public void deleteAllBranches(final LocalGitServer gitServer)
      throws IOException, GitAPIException {
    Path tempDir = baseTempFolder.newFolder("temp-local-git").toPath();
    CredentialsProvider creds = getCredentialsProviderForLocalServer();
    try (Git git = getClonedGit(tempDir, gitServer)) {
      RefSpec[] heads = git.lsRemote()
          .setTimeout(GIT_COMMAND_TIMEOUT)
          .setCredentialsProvider(getCredentialsProviderForLocalServer())
          .setHeads(true)
          .setTags(false)
          .call().stream()
          .map(ref -> new RefSpec(":" + ref.getName()))
          .toArray(RefSpec[]::new);

        //delete branches on remote
        git.push()
            .setRefSpecs(heads)
            .setTimeout(GIT_COMMAND_TIMEOUT)
            .setCredentialsProvider(creds)
            .call();
    }
  }

  private CredentialsProvider getCredentialsProviderForLocalServer() {
    return new UsernamePasswordCredentialsProvider(GIT_SERVER_USERNAME,
        MOCK_TOKEN);
  }
}
