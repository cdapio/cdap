/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.security.authorization;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.name.Named;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.common.security.AuthEnforce;
import io.cdap.cdap.common.security.AuthEnforceRewriter;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ScheduleId;
import io.cdap.cdap.proto.security.StandardPermission;

import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

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

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
    public void testSingleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    @VisibleForTesting // NOTE : tests that the presence of other annotations does not affect class rewrite
    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, 
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testMultipleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
    public void testMethodWithoutException(@Name("namespaceId") NamespaceId namespaceId) {
      // no-op
      // After class rewrite we make a call to AuthorizationEnforcer.enforce which can throw UnauthorizedException.
      // This tests that a method which does not specify throws Exception in its signature will be able to throw
      // exception during enforcement
    }

    // to test that presence of a method with AuthEnforce in class does not affect other methods which does have
    // AuthEnforce annotation
    public void testNoAuthEnforceAnnotation(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
      throw new EnforceNotCalledException();
    }

    // test AuthEnforce annotation which has multiple string parts in entities
    @AuthEnforce(entities = {"namespace", "dataset"}, enforceOn = DatasetId.class,
      permissions = StandardPermission.UPDATE)
    public void testMultipleParts(@Name("namespace") String namespace,
                                  @Name("dataset") String dataset) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // test AuthEnforce where method parameters are marked with QueryParam and PathParam
    @AuthEnforce(entities = {"namespace", "dataset"}, enforceOn = DatasetId.class,
      permissions = StandardPermission.UPDATE)
    public void testQueryPathParamAnnotations(@QueryParam("namespace") String namespace,
                                              @PathParam("dataset") String dataset) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // test the preference of Name annotation when a method parameter is marked with Name and PathParam both
    @AuthEnforce(entities = "namespace", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
    public void testMultipleAnnotationsPref(@Name("namespace") @PathParam("namespaceId") NamespaceId namespaceId)
            throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // test the preference of Name annotation when two different parameters are marked with same name but one with Name
    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
    public void testNameAnnotationPref(@Name("namespaceId") NamespaceId namespaceId,
                                       @PathParam("namespaceId") String ns)
            throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }
  }

  /**
   * Class which has different possible valid {@link AuthEnforce} annotations just like
   * {@link ValidAuthEnforceAnnotations} just to test with two valid inner classes
   */
  public class AnotherValidAuthEnforceAnnotations {

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
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
    @AuthEnforce(entities = "someEntity", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
    public void testNoParameters() throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // test that having a para name same as field name
    @AuthEnforce(entities = "someEntity", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
    public void testParaNameSameAsField(NamespaceId someEntity) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // tests that when a method parameter has Named annotation same as class field name and when specified in
    // AuthEnforce entities method parameter gets preference
    @AuthEnforce(entities = "someEntity", enforceOn = InstanceId.class, permissions = StandardPermission.UPDATE)
    public void testParaPreference(@Name("someEntity") InstanceId instanceId) throws Exception {
      // the above annotation will call enforce after class rewrite which should throw an exception.
      // If the line below is reached it means that enforce was not called as it supposed to be
      throw new EnforceNotCalledException();
    }

    // tests that when parameter has same Name annotation as the one specified in AuthEnforce annotation saying
    // this.name give preference to class field than the default method parameters
    @AuthEnforce(entities = "this.someEntity", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
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

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testEntityNameAbsence(NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation without a blank {@link Name} annotated parameter
   */
  public class BlankEntityName {

    @AuthEnforce(entities = "", enforceOn = NamespaceId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testBlankEntityName(@Name("") NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation and parameter annotated with invalid annotation {@link Named}
   */
  public class InvalidParameterAnnotationType {

    @AuthEnforce(entities = "wrongType", enforceOn = NamespaceId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testEntityNameAbsence(@Named("wrongType") NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation and two parameters with same name
   */
  public class DuplicateAnnotationName {

    @AuthEnforce(entities = "wrongType", enforceOn = DatasetId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testDuplicationAnnotationWithSameName(@Name("name") String s1, @Name("name") String s2)
            throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation have multiple parts which is not only string type
   */
  public class EntityWithString {

    @AuthEnforce(entities = {"entity", "string"}, enforceOn = DatasetId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testEntityAndString(@Name("entity") NamespaceId p1, @Name("string") String p2)
            throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with same name in {@link QueryParam} and {@link PathParam}
   */
  public class SameQueryAndPathParam {

    @AuthEnforce(entities = "wrongType", enforceOn = DatasetId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testDuplicationAnnotationWithSameName(@QueryParam("name") String s1, @PathParam("name") String s2)
            throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with invalid multiple parts
   */
  public class LessMultipleParts {

    @AuthEnforce(entities = {"namespace"}, enforceOn = DatasetId.class, permissions = StandardPermission.UPDATE)
    public void testLessMultipleParts(@Name("namespace") String namespace) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with invalid multiple parts
   */
  public class MoreMultipleParts {

    @AuthEnforce(entities = {"namespace", "artifact", "version"}, enforceOn = DatasetId.class,
      permissions = StandardPermission.GET)
    public void testMoreMultipleParts(@Name("namespace") String namespace, @Name("artifact") String artifact,
                                      @Name("version") String version) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with invalid multiple parts
   */
  public class MultipleEntityIds {

    @AuthEnforce(entities = {"namespace", "dataset"}, enforceOn = DatasetId.class, permissions = StandardPermission.GET)
    public void testMultipleEntityIds(@Name("namespace") NamespaceId namespace, @Name("artifact") DatasetId dataset)
            throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with invalid enforce on
   */
  public class InvalidAuthEnforceEntityType {

    @AuthEnforce(entities = {"schedule"}, enforceOn = ScheduleId.class, permissions = StandardPermission.GET)
    public void testInvalidEnforceOn(@Name("schedule") ScheduleId scheduleId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation without an invalid {@link Name} annotated parameter than the one
   * specified in {@link AuthEnforce#entities()}
   */
  public class InvalidEntityName {

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testWrongEntityName(@Name("wrongId") NamespaceId namespaceId) throws Exception {
      // no-op
    }
  }

  /**
   * Class which has {@link AuthEnforce} annotation with multiple parameters with same {@link Name} annotation
   */
  public class DuplicateEntityName {

    @AuthEnforce(entities = "duplicateName", enforceOn = NamespaceId.class,
      permissions = {StandardPermission.UPDATE, StandardPermission.GET})
    public void testDuplicateEntityName(@Name("duplicateName") NamespaceId namespaceId,
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

    @AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, permissions = StandardPermission.UPDATE)
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
