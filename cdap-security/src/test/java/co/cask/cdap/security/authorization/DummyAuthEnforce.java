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
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;

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

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    public void testMethodWithoutException(@Name("namespaceId") NamespaceId namespaceId) {
      // no-op
      // After class rewrite we make a call to AuthorizationEnforcer.enforce which can throw UnauthorizedException.
      // This tests that a method which does not specify throws Exception in its signature will be able to throw
      // exception during enforcement
    }

    // to test that presence of a method with AuthEnforce in classs does not affect other methods which does have
    // AuthEnforce annotation
    public void testNoAuthEnforceAnnotation(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      throw new EnforceNotCalledException();
    }
  }

  /**
   * Class which has different possible valid {@link AuthEnforce} annotations just like
   * {@link ValidAuthEnforceAnnotations} just to test with two valid inner classes
   */
  public class AnotherValidAuthEnforceAnnotations {

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    public void testSomeOtherAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation and fields
   */
  public class ValidAuthEnforceWithFields {

    public NamespaceId someEntity = new NamespaceId("ns");

    // test when method has no parameters and enforcement is one field
    @AuthEnforce(entities = "someEntity", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    public void testNoParameters() throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // test that having a para name same as field name
    @AuthEnforce(entities = "someEntity", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    public void testParaNameSameAsField(NamespaceId someEntity) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // tests that when a method parameter has Named annotation same as class field name and when specified in
    // AuthEnforce entities method parameter gets prefernce
    @AuthEnforce(entities = "someEntity", enforceOn = InstanceId.class, actions = Action.ADMIN)
    public void testParaPreference(@Name("someEntity") InstanceId instanceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // tests that when parameter has same Name annotation as the one specified in AuthEnforce annotation saying
    // this.name give preference to class field than the default method parameters
    @AuthEnforce(entities = "this.someEntity", enforceOn = NamespaceId.class, actions = Action.ADMIN)
    public void testThisClassPreference(@Name("someEntity") NamespaceId namespaceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation without a {@link Name} annotated parameter
   */
  public class AbsentEntityName {

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

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = {Action.ADMIN, Action.READ})
    public void testWrongEntityName(@Name("wrongId") NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with multiple parameters with same {@link Name} annotation
   */
  public class DuplicateEntityName {

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

  }
}
