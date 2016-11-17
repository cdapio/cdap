/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.common.security.AuthEnforce;
import co.cask.cdap.common.security.AuthEnforceRewriter;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.auth.context.AuthenticationTestContext;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;

/**
 * A Dummy class which is just an wrapper and have different inner classes with {@link AuthEnforce} annotations.
 * to test class rewrite done through {@link AuthEnforceRewriter} for the annotations. The different
 * {@link AuthEnforce} annotations are in their own independent inner classes rather than just in one class as we
 * want to test the rewrite of one annotation independent of other.
 * <p>
 * Please see tests in {@link AuthEnforceRewriterTest}
 */
public class DummyAuthEnforce {

  /**
   * Class which has different possible valid {@link AuthEnforce} annotations
   */
  public class ValidAuthEnforceAnnotations {

    // these fields are not used but here as they will be used during class rewrite
    // TODO: Remove these fields once we start supporting generating this field in class rewrite
    private final AuthorizationEnforcer authorizationEnforcer;
    private final AuthenticationContext authenticationContext;

    public ValidAuthEnforceAnnotations() {
      authenticationContext = new AuthenticationTestContext();
      authorizationEnforcer = new ExceptionAuthorizationEnforcer();
    }

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    public void testSingleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    @Deprecated // tests that the presence of other annotations does not affect class rewrite
    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = {Action.ADMIN, Action.READ})
    public void testMultipleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // to test that presence of a method with AuthEnforce in classs does not affect other methods which does have
    // AuthEnforce annotation
    public void testNoAuthEnforceAnnotation(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      throw new EnforceNotCalledException();
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation without a {@link Name} annotated parameter
   */
  public class AbsentEntityName {

    // these fields are not used but here as they will be used during class rewrite
    // TODO: Remove these fields once we start supporting generating this field in class rewrite
    private final AuthorizationEnforcer authorizationEnforcer;
    private final AuthenticationContext authenticationContext;

    public AbsentEntityName(AuthorizationEnforcer authorizationEnforcer,
                            AuthenticationContext authenticationContext) {
      this.authorizationEnforcer = authorizationEnforcer;
      this.authenticationContext = authenticationContext;
    }

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = {Action.ADMIN, Action.READ})
    public void testEntityNameAbsence(NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation without an invalid {@link Name} annotated parameter than the one
   * specified in {@link AuthEnforce#entities()}
   */
  public class InvalidEntityName {

    // these fields are not used but here as they will be used during class rewrite
    // TODO: Remove these fields once we start supporting generating this field in class rewrite
    private final AuthorizationEnforcer authorizationEnforcer;
    private final AuthenticationContext authenticationContext;

    public InvalidEntityName(AuthorizationEnforcer authorizationEnforcer,
                             AuthenticationContext authenticationContext) {
      this.authorizationEnforcer = authorizationEnforcer;
      this.authenticationContext = authenticationContext;
    }

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = {Action.ADMIN, Action.READ})
    public void testWrongEntityName(@Name("wrongId") NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with multiple parameters with same {@link Name} annotation
   */
  public class DuplicateEntityName {

    // these fields are not used but here as they will be used during class rewrite
    // TODO: Remove these fields once we start supporting generating this field in class rewrite
    private final AuthorizationEnforcer authorizationEnforcer;
    private final AuthenticationContext authenticationContext;

    public DuplicateEntityName(AuthorizationEnforcer authorizationEnforcer,
                               AuthenticationContext authenticationContext) {
      this.authorizationEnforcer = authorizationEnforcer;
      this.authenticationContext = authenticationContext;
    }

    @AuthEnforce(entities = "duplicateName", enforceOn = NamespaceId.class, actions = {Action.ADMIN, Action.READ})
    public void testDuplicatEntityName(@Name("duplicateName") NamespaceId namespaceId,
                                       @Name("duplicateName") NamespaceId anotherNamespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which does not have {@link AuthEnforce}
   */
  public class ClassWithoutAuthEnforce {

    // these fields are not used but here as they will be used during class rewrite
    // TODO: Remove these fields once we start supporting generating this field in class rewrite
    private final AuthorizationEnforcer authorizationEnforcer;
    private final AuthenticationContext authenticationContext;

    public ClassWithoutAuthEnforce() {
      authenticationContext = new AuthenticationTestContext();
      authorizationEnforcer = new ExceptionAuthorizationEnforcer();
    }

    public void methodWithoutAuthEnforce(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      throw new EnforceNotCalledException();
    }
  }

  /**
   * An interface which has {@link AuthEnforce} annotation
   */
  public interface InterfaceWithAuthAnnotation {
    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    void interfaceMethodWithAuthEnforce(@Name("namespaceId") NamespaceId namespaceId) throws Exception;
  }

  /**
   * A class implementing an interface with {@link AuthEnforce} annotation
   */
  public class ClassImplementingInterfaceWithAuthAnnotation implements InterfaceWithAuthAnnotation {

    @Override
    public void interfaceMethodWithAuthEnforce(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      throw new EnforceNotCalledException();
    }
  }

  /**
   * Just a dummy exception which is thrown is authorization enforcement was not done
   */
  public class EnforceNotCalledException extends Exception {
    // dummy
  }
}
