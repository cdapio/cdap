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
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfigValidationException;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.transport.UsernamePasswordCredentialsProvider;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Tests for {@link  RepositoryManager}.
 */
public class RepositoryManagerTest {
  private static final String DEFAULT_BRANCH_NAME = "develop";
  private static final String GIT_SERVER_USERNAME = "oauth2";
  private static final String MOCK_TOKEN = UUID.randomUUID().toString();
  private static final String NAMESPACE = "namespace1";
  private static final String GITHUB_TOKEN_NAME = "github-pat";
  private static final int GIT_COMMAND_TIMEOUT = 2;
  @Mock
  private SecureStore secureStore;
  private CConfiguration cConf;
  public TemporaryFolder tempFolder = new TemporaryFolder();
  public LocalGitServer gitServer =
    new LocalGitServer(GIT_SERVER_USERNAME, MOCK_TOKEN, 0, DEFAULT_BRANCH_NAME, tempFolder);
  @Rule
  public RuleChain chain = RuleChain.outerRule(tempFolder).around(gitServer);

  @Before
  public void beforeEach() throws Exception {
    MockitoAnnotations.initMocks(this);
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, MOCK_TOKEN.getBytes(StandardCharsets.UTF_8)));
    cConf = CConfiguration.create();
    cConf.setInt(Constants.SourceControlManagement.GIT_COMMAND_TIMEOUT_SECONDS, GIT_COMMAND_TIMEOUT);
    cConf.set(Constants.SourceControlManagement.GIT_REPOSITORIES_CLONE_DIRECTORY_PATH,
              tempFolder.newFolder("repository").getAbsolutePath());
  }

  @Test
  public void testValidateIncorrectKeyName() throws Exception {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME + "invalid")
      .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertTrue(e.getMessage().contains("Failed to get authentication credentials"));
    }
  }

  @Test
  public void testValidateInvalidToken() throws Exception {
    SourceControlConfig sourceControlConfig = getSourceControlConfig();
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, "invalid-token".getBytes(StandardCharsets.UTF_8)));
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertTrue(e.getMessage().contains("not authorized"));
    }
  }

  @Test
  public void testValidateInvalidBranchName() throws Exception {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop-invalid")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
      Assert.fail();
    } catch (RepositoryConfigValidationException e) {
      Assert.assertTrue(e.getMessage().contains("Default branch not found in remote repository"));
    }
  }

  @Test
  public void testValidateNullBranchName() throws Exception {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    RepositoryManager.validateConfig(secureStore, sourceControlConfig);
  }

  @Test
  public void testValidateSuccess() throws Exception {
    RepositoryManager.validateConfig(secureStore, getSourceControlConfig());
  }

  @Test
  public void testGetFileHashSuccess() throws Exception {
    String fileName = "pipeline.json";
    Path path = Paths.get(fileName);
    String fileContents = "{name: pipeline}";
    addFileToGit(path, fileContents);
    RepositoryManager manager = getRepositoryManager();
    String commitID = manager.cloneRemote();
    String fileHash = manager.getFileHash(path, commitID);

    String expectedHash = getGitStyleHash(fileContents);
    Assert.assertEquals(expectedHash, fileHash);
    // Change the file, but don't commit. The hash should remain the same.
    Files.write(manager.getBasePath().resolve(fileName), "change 1".getBytes(StandardCharsets.UTF_8));
    fileHash = manager.getFileHash(path, manager.resolveHead().getName());
    Assert.assertEquals(expectedHash, fileHash);
    manager.close();
    // Change the file contents and commit, the hash should also change.
    addFileToGit(path, "change 2");
    commitID = manager.cloneRemote();
    Assert.assertNotEquals(manager.getFileHash(path, commitID), expectedHash);
    manager.close();
  }

  @Test(expected = NotFoundException.class)
  public void testGetFileHashInvlidPath() throws Exception {
    try (RepositoryManager manager = getRepositoryManager();) {
      String commitID = manager.cloneRemote();
      manager.getFileHash(Paths.get("data-pipelines", "pipeline.json"), commitID);
    }
  }

  @Test
  public void testGetFileHashChangedFile() throws Exception {
    // Add two files to git.
    String fileName1 = "file1.json";
    String fileContents1 = "abc";
    String fileContents2 = "def";
    Path path = Paths.get(fileName1);
    addFileToGit(path, fileContents1);
    // check file hash of link.
    RepositoryManager manager = getRepositoryManager();
    manager.cloneRemote();
    String commitID = manager.resolveHead().getName();
    String hash = manager.getFileHash(path, commitID);
    // Change file contents, but don't commit.
    Path absoluteRepoPath = manager.getRepositoryRoot().resolve(path);
    Files.write(absoluteRepoPath, fileContents2.getBytes(StandardCharsets.UTF_8));
    // Check the file hash of the link, it should be unchanged.
    Assert.assertEquals(hash, manager.getFileHash(path, commitID));
    // Check that working directory changes are still present.
    Assert.assertEquals(new String(Files.readAllBytes(absoluteRepoPath), StandardCharsets.UTF_8), fileContents2);
    manager.close();
    // Commit the symlink change.
    addFileToGit(path, fileContents2);
    manager.cloneRemote();
    // Check the file hash at previous commit.
    Assert.assertEquals(hash, manager.getFileHash(path, commitID));
    // Check the file hash at head.
    Assert.assertNotEquals(hash, manager.getFileHash(path, manager.resolveHead().getName()));
    manager.close();
  }

  @Test(expected = IOException.class)
  public void testGetFileHashInvalidCommitID() throws Exception {
    String fileName = "pipeline.json";
    Path path = Paths.get(fileName);
    addFileToGit(path, "{name: pipeline}");
    RepositoryManager manager = getRepositoryManager();
    manager.cloneRemote();
    try {
      manager.getFileHash(path, "7edf45b8cab741caf657e71bd30464860fa56789");
    } finally {
      manager.close();
    }
  }

  private RepositoryConfig.Builder getRepositoryConfigBuilder() {
    return new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(gitServer.getServerURL() + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME);
  }

  /**
   * Creates a valid {@link SourceControlConfig}.
   *
   * @return the {@link SourceControlConfig}.
   */
  private SourceControlConfig getSourceControlConfig() {
    return new SourceControlConfig(new NamespaceId(NAMESPACE), getRepositoryConfigBuilder().build(), cConf);
  }

  /**
   * Creates a valid {@link RepositoryManager}.
   *
   * @return the {@link RepositoryManager}.
   */
  private RepositoryManager getRepositoryManager() {
    return new RepositoryManager(secureStore, cConf, new NamespaceId(NAMESPACE), getRepositoryConfigBuilder().build());
  }

  /**
   * Clones the Git repository hosted by the {@link LocalGitServer}.
   *
   * @param dir the directory to clone the repository in.
   * @return the cloned {@link Git} object.
   */
  private Git getClonedGit(Path dir) throws GitAPIException {
    return Git.cloneRepository()
      .setURI(gitServer.getServerURL() + "ignored")
      .setDirectory(dir.toFile())
      .setBranch(DEFAULT_BRANCH_NAME)
      .setTimeout(GIT_COMMAND_TIMEOUT)
      .setCredentialsProvider(new UsernamePasswordCredentialsProvider(GIT_SERVER_USERNAME, MOCK_TOKEN))
      .call();
  }

  /**
   * Adds a file to the git repository hosted by the {@link LocalGitServer}.
   *
   * @param relativePath path relative to git repo root.
   * @param contents     the contents of the file.
   */
  private void addFileToGit(Path relativePath, String contents) throws GitAPIException, IOException {
    Path tempDirPath = tempFolder.newFolder("temp-local-git").toPath();
    Git localGit = getClonedGit(tempDirPath);
    Path absolutePath = tempDirPath.resolve(relativePath);
    // Create parent directories if they don't exist.
    Files.createDirectories(absolutePath.getParent());
    Files.write(absolutePath, contents.getBytes(StandardCharsets.UTF_8));
    localGit.add().addFilepattern(".").call();
    localGit.commit().setMessage("Adding " + relativePath).call();
    localGit.push()
      .setTimeout(GIT_COMMAND_TIMEOUT)
      .setCredentialsProvider(new UsernamePasswordCredentialsProvider(GIT_SERVER_USERNAME, MOCK_TOKEN))
      .call();
    localGit.close();
    DirUtils.deleteDirectoryContents(tempDirPath.toFile());
  }

  /**
   * Calculates the hash for provided file contents in a similar way to Git.
   */
  private String getGitStyleHash(String fileContents) {
    // Git prefixes the object with "blob ", followed by the length (as a human-readable integer), followed by a NUL
    // character and takes the sha1 hash to find the file hash.
    // See https://stackoverflow.com/a/7225329.
    return Hashing.sha1()
      .hashString("blob " + fileContents.length() + "\0" + fileContents, StandardCharsets.UTF_8)
      .toString();
  }

  @Test
  public void testCommitAndPushSuccess() throws Exception {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();

    try (RepositoryManager manager = new RepositoryManager(secureStore, cConf, new NamespaceId(NAMESPACE), config)) {
      manager.cloneRemote();
      CommitMeta commitMeta = new CommitMeta("author", "committer", 100, "message");
      Path filePath = manager.getBasePath().resolve("file1");
      String fileContent = "content";

      Files.write(filePath, fileContent.getBytes(StandardCharsets.UTF_8));
      manager.commitAndPush(commitMeta, manager.getBasePath().relativize(filePath));
      verifyCommit(filePath, fileContent, commitMeta);
    }
  }

  @Test(expected = NoChangesToPushException.class)
  public void testCommitAndPushNoChangeSuccess() throws Exception {
    String pathPrefix = "prefix";
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setPathPrefix(pathPrefix)
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();

    try (RepositoryManager manager = new RepositoryManager(secureStore, cConf, new NamespaceId(NAMESPACE), config)) {
      manager.cloneRemote();
      CommitMeta commitMeta = new CommitMeta("author", "committer", 100, "message");
      manager.commitAndPush(commitMeta, Paths.get(pathPrefix));

      verifyNoCommit();
    }
  }

  @Test(expected = GitAPIException.class)
  public void testCommitAndPushFails() throws Exception {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();

    try (RepositoryManager manager = new RepositoryManager(secureStore, cConf, new NamespaceId(NAMESPACE), config)) {
      manager.cloneRemote();
      CommitMeta commitMeta = new CommitMeta("author", "committer", 100, "message");
      Path filePath = manager.getBasePath().resolve("file1");
      String fileContent = "content";

      Files.write(filePath, fileContent.getBytes(StandardCharsets.UTF_8));
      gitServer.after();
      manager.commitAndPush(commitMeta, manager.getBasePath().relativize(filePath));
    }
  }

  private void verifyNoCommit() throws GitAPIException, IOException {
    Path tempDirPath = tempFolder.newFolder("temp-local-git-verify").toPath();
    Git localGit = getClonedGit(tempDirPath);
    List<RevCommit> commits =
      StreamSupport.stream(localGit.log().all().call().spliterator(), false).collect(Collectors.toList());
    Assert.assertEquals(1, commits.size());
  }

  private void verifyCommit(Path pathFromRepoRoot, String expectedContent, CommitMeta commitMeta) throws
    GitAPIException, IOException {
    Path tempDirPath = tempFolder.newFolder("temp-local-git-verify").toPath();
    Git localGit = getClonedGit(tempDirPath);
    Path filePath = tempDirPath.resolve(pathFromRepoRoot);

    String actualContent = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
    Assert.assertEquals(expectedContent, actualContent);

    List<RevCommit> commits =
      StreamSupport.stream(localGit.log().all().call().spliterator(), false).collect(Collectors.toList());
    Assert.assertEquals(2, commits.size());
    RevCommit latestCommit = commits.get(0);

    Assert.assertEquals(latestCommit.getFullMessage(), commitMeta.getMessage());
    Assert.assertEquals(commitMeta.getAuthor(), latestCommit.getAuthorIdent().getName());
    Assert.assertEquals(commitMeta.getCommitter(), latestCommit.getCommitterIdent().getName());

    localGit.close();
    DirUtils.deleteDirectoryContents(tempDirPath.toFile());
  }
}
