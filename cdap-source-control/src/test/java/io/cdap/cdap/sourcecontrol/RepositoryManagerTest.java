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
import java.util.ArrayList;
import java.util.Collections;
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
  public void testValidateIncorrectKeyName() throws IOException {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME + "invalid")
      .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    boolean exceptionCaught = false;
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
    } catch (RepositoryConfigValidationException e) {
      exceptionCaught = true;
      Assert.assertTrue(e.getMessage().contains("Failed to get authentication credentials"));
    }
    Assert.assertTrue(exceptionCaught);
  }

  @Test
  public void testValidateInvalidToken() throws Exception {
    SourceControlConfig sourceControlConfig = getSourceControlConfig();
    Mockito.when(secureStore.get(NAMESPACE, GITHUB_TOKEN_NAME))
      .thenReturn(new SecureStoreData(null, "invalid-token".getBytes(StandardCharsets.UTF_8)));
    boolean exceptionCaught = false;
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
    } catch (RepositoryConfigValidationException e) {
      exceptionCaught = true;
      Assert.assertTrue(e.getMessage().contains("not authorized"));
    }
    Assert.assertTrue(exceptionCaught);
  }

  @Test
  public void testValidateInvalidBranchName() throws IOException {
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop-invalid")
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);
    boolean exceptionCaught = false;
    try {
      RepositoryManager.validateConfig(secureStore, sourceControlConfig);
    } catch (RepositoryConfigValidationException e) {
      exceptionCaught = true;
      Assert.assertTrue(e.getMessage().contains("Default branch not found in remote repository"));
    }
    Assert.assertTrue(exceptionCaught);
  }

  @Test
  public void testValidateNullBranchName() throws IOException {
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
  public void testValidateSuccess() throws IOException {
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
    fileHash = manager.getFileHash(path, commitID);
    Assert.assertNotEquals(fileHash, expectedHash);
    manager.close();
  }

  @Test
  public void testGetFileHashChangedFile() throws Exception {
    // Add two files to git.
    String fileName1 = "file1.json";
    String fileContents1 = "abc";
    String fileName2 = "file2.json";
    String fileContents2 = "def";
    addFileToGit(Paths.get(fileName1), fileContents1);
    addFileToGit(Paths.get(fileName2), fileContents2);
    // Add a symlink pointing to file1.
    String symlinkFileName = "link.json";
    Path symlinkPath = Paths.get(symlinkFileName);
    addSymbolicLinkToGit(symlinkPath, Paths.get(fileName1));
    // check file hash of link.
    RepositoryManager manager = getRepositoryManager();
    manager.cloneRemote();
    String commitID = manager.resolveHead().getName();
    String hash = manager.getFileHash(symlinkPath, commitID);
    // Change the symlink to point to file 2, but don't commit.
    Path absoluteLinkPath = manager.getRepositoryRoot().resolve(symlinkFileName);
    Path absoluteTargetPath = manager.getRepositoryRoot().resolve(fileName2);
    Files.delete(absoluteLinkPath);
    Files.createSymbolicLink(absoluteLinkPath, absoluteLinkPath.getParent().relativize(absoluteTargetPath));
    // Check the file hash of the link, it should be unchanged.
    Assert.assertEquals(hash, manager.getFileHash(symlinkPath, commitID));
    // Check that working directory changes are still present.
    Assert.assertEquals(absoluteLinkPath.toRealPath(), absoluteTargetPath);
    manager.close();
    // Commit the symlink change.
    addSymbolicLinkToGit(symlinkPath, Paths.get(fileName2));
    manager.cloneRemote();
    // Check the file hash at previous commit.
    Assert.assertEquals(hash, manager.getFileHash(symlinkPath, commitID));
    // Check the file hash at head.
    Assert.assertNotEquals(hash, manager.getFileHash(symlinkPath, manager.resolveHead().getName()));
    manager.close();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetFileHashDirectory() throws Exception {
    String fileName = "pipeline.json";
    String dirName = "data-pipelines";
    // Add a regular file inside a directory.
    Path regularFilePath = Paths.get(dirName, fileName);
    addFileToGit(regularFilePath, "{name: pipeline}");
    RepositoryManager manager = getRepositoryManager();
    String commitID = manager.cloneRemote();
    try {
      manager.getFileHash(Paths.get(dirName), commitID);
    } finally {
      manager.close();
    }
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

  @Test
  public void testGetFileHashSymlinks() throws Exception {
    String fileName = "pipeline.json";
    // Add a regular file inside a directory.
    Path regularFilePath = Paths.get(fileName);
    String contents = "{name: pipeline}";
    addFileToGit(regularFilePath, contents);
    // Add a symbolic link.
    Path symlinkDirectory = Paths.get("cdap", "applications");
    String symLinkFileName1 = "pipeline-link-1.json";
    String symLinkFileName2 = "pipeline-link-2.json";
    Path symlinkPath1 = symlinkDirectory.resolve(symLinkFileName1);
    addSymbolicLinkToGit(symlinkPath1, regularFilePath);
    // Create a symbolic link to a symbolic link (a->b->c).
    Path symlinkPath2 = symlinkDirectory.resolve(symLinkFileName2);
    addSymbolicLinkToGit(symlinkPath2, symlinkPath1);

    RepositoryConfig repositoryConfig = getRepositoryConfigBuilder().setPathPrefix("cdap/applications").build();
    RepositoryManager manager = new RepositoryManager(secureStore, cConf, new NamespaceId(NAMESPACE), repositoryConfig);
    String commitID = manager.cloneRemote();
    String expectedHash = getGitStyleHash(contents);
    // The hash of the original file, and both the symlinks should be equal.
    Assert.assertEquals(manager.getFileHash(symlinkPath1, commitID), expectedHash);
    Assert.assertEquals(manager.getFileHash(symlinkPath2, commitID), expectedHash);
    manager.close();
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
   * Adds a symbolic link to the git repository hosted by the {@link LocalGitServer}. This overwrites the link file
   * if it already exists.
   *
   * @param relativeLinkPath   relative path to create the symlink.
   * @param relativeTargetPath relative path to an existing file in the Git repository.
   */
  private void addSymbolicLinkToGit(Path relativeLinkPath, Path relativeTargetPath) throws GitAPIException,
    IOException {
    Path tempDirPath = tempFolder.newFolder("temp-local-git").toPath();
    Git localGit = getClonedGit(tempDirPath);
    Path absoluteTargetPath = tempDirPath.resolve(relativeTargetPath);
    Path absoluteLinkPath = tempDirPath.resolve(relativeLinkPath);
    // Create parent directories for the link if they don't exist.
    Files.createDirectories(absoluteLinkPath.getParent());
    if (Files.exists(absoluteLinkPath)) {
      Files.delete(absoluteLinkPath);
    }
    // Create a relative link so that it remains valid irrespective of where the repository is cloned.
    Files.createSymbolicLink(absoluteLinkPath, absoluteLinkPath.getParent().relativize(absoluteTargetPath));
    localGit.add().addFilepattern(".").call();
    localGit.commit().setMessage("Adding Symbolic link: " + relativeTargetPath).call();
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

    try (RepositoryManager manager = new RepositoryManager(secureStore, cConf,
                                                           new NamespaceId(NAMESPACE), config)) {
      manager.cloneRemote();
      CommitMeta commitMeta = new CommitMeta("author", "committer", 100, "message");
      Path filePath = manager.getBasePath().resolve("file1");
      String fileContent = "content";

      Files.write(filePath, fileContent.getBytes(StandardCharsets.UTF_8));
      manager.commitAndPush(commitMeta, new ArrayList<Path>() {{
        add(manager.getBasePath().relativize(filePath));
      }});
      verifyCommit(filePath, fileContent, commitMeta);
    }
  }

  @Test(expected = UnexpectedRepositoryChangesException.class)
  public void testCommitAndPushUnstagedChangesFailure() throws Exception {
    String pathPrefix = "prefix";
    String serverURL = gitServer.getServerURL();
    RepositoryConfig config = new RepositoryConfig.Builder().setProvider(Provider.GITHUB)
      .setLink(serverURL + "ignored")
      .setDefaultBranch("develop")
      .setPathPrefix(pathPrefix)
      .setAuthType(AuthType.PAT)
      .setTokenName(GITHUB_TOKEN_NAME)
      .build();
    SourceControlConfig sourceControlConfig = new SourceControlConfig(new NamespaceId(NAMESPACE), config, cConf);

    try (RepositoryManager manager = new RepositoryManager(secureStore, cConf, new NamespaceId(NAMESPACE),
                                                           config)) {
      manager.cloneRemote();
      CommitMeta commitMeta = new CommitMeta("author", "committer", 100, "message");

      Files.createDirectories(manager.getBasePath());
      Path filePath = manager.getBasePath().resolve("file1");
      String fileContent = "content";
      Files.write(filePath, fileContent.getBytes(StandardCharsets.UTF_8));

      Path wrongFilePath = manager.getBasePath().getParent().resolve("file1");
      Files.write(wrongFilePath, fileContent.getBytes(StandardCharsets.UTF_8));

      manager.commitAndPush(commitMeta, new ArrayList<Path>() {{
        add(manager.getBasePath().relativize(filePath));
      }});
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
      manager.commitAndPush(commitMeta, Collections.emptyList());

      verifyNoCommit();
    }
  }

  private void verifyNoCommit() throws GitAPIException, IOException {
    Path tempDirPath = tempFolder.newFolder("temp-local-git-verify").toPath();
    Git localGit = getClonedGit(tempDirPath);
    List<RevCommit> commits = StreamSupport.stream(localGit.log().all().call().spliterator(), false).
      collect(Collectors.toList());
    Assert.assertEquals(1, commits.size());
  }

  private void verifyCommit(Path pathFromRepoRoot, String expectedContent, CommitMeta commitMeta)
    throws GitAPIException, IOException {
    Path tempDirPath = tempFolder.newFolder("temp-local-git-verify").toPath();
    Git localGit = getClonedGit(tempDirPath);
    Path filePath = tempDirPath.resolve(pathFromRepoRoot);

    String actualContent = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
    Assert.assertEquals(expectedContent, actualContent);

    List<RevCommit> commits = StreamSupport.stream(localGit.log().all().call().spliterator(), false)
      .collect(Collectors.toList());
    Assert.assertEquals(2, commits.size());
    RevCommit latestCommit = commits.get(0);

    Assert.assertEquals(latestCommit.getFullMessage(), commitMeta.getMessage());
    Assert.assertEquals(commitMeta.getAuthor(), latestCommit.getAuthorIdent().getName());
    Assert.assertEquals(commitMeta.getCommitter(), latestCommit.getCommitterIdent().getName());

    localGit.close();
    DirUtils.deleteDirectoryContents(tempDirPath.toFile());
  }

}
