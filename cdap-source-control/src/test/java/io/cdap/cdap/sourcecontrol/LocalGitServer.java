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

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.HashLoginService;
import org.eclipse.jetty.security.UserStore;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.security.Password;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.http.server.GitServlet;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A local server with authentication that hosts a Git repository for tests.
 */
public class LocalGitServer extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(LocalGitServer.class);
  private final Server server;
  private final GitServlet gitServlet;
  private final String username;
  private final String branchName;
  private final String password;
  private final int port;
  private final TemporaryFolder temporaryFolder;
  private ServerConnector serverConnector;
  private Git bareGit;
  private Git testGit;

  /**
   * Creates a {@link LocalGitServer}
   *
   * @param username   username for authentication.
   * @param password   password for authentication.
   * @param port       port to start the server on. Use 0 for a random port.
   * @param branchName branch to create in the git repository initially.
   * @param tempFolder Provider of temporary folders for creating get repositories.
   */
  public LocalGitServer(String username, String password, int port, String branchName, TemporaryFolder tempFolder) {
    this.server = new Server();
    this.gitServlet = new GitServlet();
    this.username = username;
    this.password = password;
    this.port = port;
    this.branchName = branchName;
    this.temporaryFolder = tempFolder;
  }

  @Nullable
  public String getServerUrl() {
    return serverConnector == null ? null : String.format("http://localhost:%s/", serverConnector.getLocalPort());
  }

  private void start() throws Exception {
    // Initialize git repositories.
    File bareRepoDir = temporaryFolder.newFolder("bare_repo");
    // Bare git repo to serve as a remote for testing.
    bareGit = Git.init().setDirectory(bareRepoDir).setBare(true).call();
    File testRepoDir = temporaryFolder.newFolder("temp_test_repo");
    // Git used to push a new branch to the bare repo.
    testGit = Git.init().setDirectory(testRepoDir).call();

    // We need to initialize history by committing before switching branches.
    Files.write(testRepoDir.toPath().resolve("abc.txt"), "content".getBytes());
    testGit.add().addFilepattern(".").call();
    testGit.commit().setMessage("main_message").call();
    testGit.checkout().setCreateBranch(true).setName(branchName).call();
    testGit.push().setPushAll().setRemote(bareRepoDir.toPath().toString()).call();

    // Always return the same repository.
    gitServlet.setRepositoryResolver((HttpServletRequest req, String name) -> bareGit.getRepository());
    // Git requests try to authenticate as admin by default, so set that as the required
    // security role for all paths.
    String[] roles = {"admin"};
    Constraint constraint = new Constraint();
    constraint.setRoles(roles);
    constraint.setAuthenticate(true);
    constraint.setName("auth");

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setPathSpec("/*");
    constraintMapping.setConstraint(constraint);

    List<ConstraintMapping> constraintMappings = Collections.singletonList(constraintMapping);
    Set<String> constraintRoles = Collections.singleton("admin");

    // Setup authentication for a single user whose username and password is provided in the constructor.
    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
    securityHandler.setConstraintMappings(constraintMappings, constraintRoles);
    securityHandler.setAuthenticator(new BasicAuthenticator());
    UserStore userStore = new UserStore();
    userStore.addUser(username, new Password(password), roles);
    HashLoginService loginService = new HashLoginService("fakeRealm");
    loginService.setUserStore(userStore);
    securityHandler.setLoginService(loginService);

    ServletHandler servletHandler = new ServletHandler();
    // Add the git servlet behind the authentication servlet handler so that only authenticated requests can access
    // the git repository.
    servletHandler.addServletWithMapping(new ServletHolder(gitServlet), "/*");
    securityHandler.setHandler(servletHandler);
    server.setHandler(securityHandler);

    serverConnector = new ServerConnector(server);
    serverConnector.setPort(port);
    server.addConnector(serverConnector);
    server.start();
  }

  /**
   * Stops a running server.
   */
  private void stop() throws Exception {
    if (server.isRunning()) {
      server.stop();
    }
    if (bareGit != null) {
      bareGit.close();
    }
    if (testGit != null) {
      testGit.close();
    }
    testGit = null;
    bareGit = null;
    serverConnector = null;
  }

  @Override
  protected void before() throws Exception {
    start();
  }

  @Override
  public void after() {
    try {
      stop();
    } catch (Exception e) {
      LOG.warn("Ignoring exception while stopping git server.", e);
    }
  }
}
