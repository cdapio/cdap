package com.continuuity.security.server;

import com.google.inject.Inject;
import org.mortbay.jetty.security.Constraint;
import org.mortbay.jetty.security.ConstraintMapping;
import org.mortbay.jetty.security.HashUserRealm;
import org.mortbay.jetty.security.SecurityHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handler for basic authentication of users.
 */
public class BasicAuthenticationHandler extends SecurityHandler {
  private static final Logger LOG = LoggerFactory.getLogger(BasicAuthenticationHandler.class);
  private String[] roles;

  @Inject
  public BasicAuthenticationHandler() {
    super();
    this.roles = new String[]{ "user", "admin", "moderator" };
    try {
      Constraint constraint = new Constraint();
      constraint.setName(Constraint.__BASIC_AUTH);
      constraint.setRoles(roles);
      constraint.setAuthenticate(true);

      ConstraintMapping constraintMapping = new ConstraintMapping();
      constraintMapping.setConstraint(constraint);
      constraintMapping.setPathSpec("/*");

      String realmFile = getClass().getResource("/realm.properties").getPath();
      this.setUserRealm(new HashUserRealm("userRealm", realmFile));
      this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
    } catch (Exception e) {
      LOG.error("Error creating BasicAuthenticationHandler.");
      LOG.error(e.getMessage());
    }

  }
}
