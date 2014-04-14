package com.continuuity.security.server;

import com.continuuity.common.conf.Constants;
import com.google.inject.Inject;
import org.mortbay.jetty.security.Constraint;
import org.mortbay.jetty.security.ConstraintMapping;
import org.mortbay.jetty.security.HashUserRealm;
import org.mortbay.jetty.security.SecurityHandler;

import java.net.URL;

/**
 * Handler for basic authentication of users.
 */
public class BasicAuthenticationHandler extends SecurityHandler {

  @Inject
  public BasicAuthenticationHandler() throws Exception {
    super();

    String[] roles = Constants.Security.BASIC_USER_ROLES;
    Constraint constraint = new Constraint();
    constraint.setName(Constraint.__BASIC_AUTH);
    constraint.setRoles(roles);
    constraint.setAuthenticate(true);

    ConstraintMapping constraintMapping = new ConstraintMapping();
    constraintMapping.setConstraint(constraint);
    constraintMapping.setPathSpec("/*");

    URL realmFile = getClass().getResource("/realm.properties");
    this.setUserRealm(new HashUserRealm("userRealm", realmFile.toExternalForm()));
    this.setConstraintMappings(new ConstraintMapping[]{constraintMapping});
  }
}
