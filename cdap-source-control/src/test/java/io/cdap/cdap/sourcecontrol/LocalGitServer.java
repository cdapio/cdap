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
import org.eclipse.jgit.http.server.GitServlet;
import org.eclipse.jgit.lib.Repository;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;

/**
 * A local server with authentication that hosts a Git repository for tests.
 */
public class LocalGitServer {
  private final Server server;
  private final GitServlet gitServlet;
  private final Repository repository;
  private final String username;
  private final String password;
  private ServerConnector serverConnector;
  private final  int port;

  /**
   * Creates a {@link LocalGitServer}
   * @param username username for authentication.
   * @param password password for authentication.
   * @param port port to start the server on. Use 0 for a random port.
   * @param repository the git repository to serve through the server.
   */
  public LocalGitServer(String username, String password, int port, Repository repository) {
    this.server = new Server();
    this.gitServlet = new GitServlet();
    this.repository = repository;
    this.username = username;
    this.password = password;
    this.port = port;
  }

  @Nullable
  public String getServerURL() {
    return serverConnector == null ? null : String.format("http://localhost:%s/", serverConnector.getLocalPort());
  }

  public void start() throws Exception {
    // Always return the same repository.
    gitServlet.setRepositoryResolver((HttpServletRequest req, String name) -> repository);
    // Git requests try to authenticate as admin by default, so set that as the required
    // security role for all paths.
    Set<String> constraintRoles = Collections.singleton("admin");

    String[] roles = {"admin"};
    Constraint constraint = new Constraint();
    constraint.setRoles(roles);
    constraint.setAuthenticate(true);
    constraint.setName("auth");

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setPathSpec("/*");
    constraintMapping.setConstraint(constraint);

    List<ConstraintMapping> constraintMappings = Collections.singletonList(constraintMapping);

    ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
    securityHandler.setConstraintMappings(constraintMappings, constraintRoles);
    securityHandler.setAuthenticator(new BasicAuthenticator());
    UserStore userStore = new UserStore();
    userStore.addUser(username, new Password(password), roles);
    HashLoginService loginService = new HashLoginService("fakeRealm");
    loginService.setUserStore(userStore);
    securityHandler.setLoginService(loginService);

    ServletHandler servletHandler = new ServletHandler();
    servletHandler.addServletWithMapping(new ServletHolder(gitServlet), "/*");
    securityHandler.setHandler(servletHandler);
    server.setHandler(securityHandler);

    serverConnector = new ServerConnector(server);
    serverConnector.setPort(port);
    server.addConnector(serverConnector);
    server.start();
  }

  // Stops a running server.
  public void stop() throws Exception {
    server.stop();
    serverConnector = null;
  }
}
