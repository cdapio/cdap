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

import co.cask.cdap.common.security.AuthEnforce;
import co.cask.cdap.common.security.AuthEnforceRewriter;
import co.cask.cdap.internal.asm.ByteCodeClassLoader;
import co.cask.cdap.internal.asm.ClassDefinition;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationTestContext;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import org.junit.Assert;
import org.junit.Test;
import org.objectweb.asm.Type;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;

/**
 * Tests {@link AuthEnforceRewriter} class rewriting for methods annotated with {@link AuthEnforce}. Uses different
 * possibilities of {@link AuthEnforce} annotation from {@link DummyAuthEnforce}.
 */
public class AuthEnforceRewriterTest {

  @Test
  public void test() throws Exception {
    ByteCodeClassLoader classLoader = new ByteCodeClassLoader(getClass().getClassLoader());
    classLoader.addClass(rewrite(DummyAuthEnforce.ValidAuthEnforceAnnotations.class));
    classLoader.addClass(rewrite(DummyAuthEnforce.AnotherValidAuthEnforceAnnotations.class));
    classLoader.addClass(rewrite(DummyAuthEnforce.ClassImplementingInterfaceWithAuthAnnotation.class));
    classLoader.addClass(rewrite(DummyAuthEnforce.ClassWithoutAuthEnforce.class));
    classLoader.addClass(rewrite(DummyAuthEnforce.ValidAuthEnforceWithFields.class));

    // Need to invoke the method on the object created from the rewritten class in the classloader since trying to
    // cast it here to DummyAuthEnforce will fail since the object is created from a class which was loaded from a
    // different classloader.
    Class<?> cls = classLoader.loadClass(DummyAuthEnforce.ValidAuthEnforceAnnotations.class.getName());
    Object rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    // tests a valid AuthEnforce annotation which has single action
    testRewrite(getMethod(cls, "testSingleAction", NamespaceId.class), rewrittenObject,
                ExceptionAuthorizationEnforcer.ExpectedException.class, NamespaceId.DEFAULT);
    // tests a valid AuthEnforce annotation which has multiple action
    testRewrite(getMethod(cls, "testMultipleAction", NamespaceId.class), rewrittenObject,
                ExceptionAuthorizationEnforcer.ExpectedException.class, NamespaceId.DEFAULT);
    // test that the class rewrite did not affect other non annotated methods
    testRewrite(getMethod(cls, "testNoAuthEnforceAnnotation", NamespaceId.class), rewrittenObject,
                DummyAuthEnforce.EnforceNotCalledException.class, NamespaceId.DEFAULT);
    // test that the class rewrite works for method whose signature does not specify throws exception
    testRewrite(getMethod(cls, "testMethodWithoutException", NamespaceId.class), rewrittenObject,
                ExceptionAuthorizationEnforcer.ExpectedException.class, NamespaceId.DEFAULT);

    // tests that class rewriting does not happen if an interface has a method with AuthEnforce
    cls = classLoader.loadClass(DummyAuthEnforce.ClassImplementingInterfaceWithAuthAnnotation.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(getMethod(cls, "interfaceMethodWithAuthEnforce", NamespaceId.class), rewrittenObject,
                DummyAuthEnforce.EnforceNotCalledException.class, NamespaceId.DEFAULT);

    // test that class rewriting does not happen for classes which does not have AuthEnforce annotation on its method
    cls = classLoader.loadClass(DummyAuthEnforce.ClassWithoutAuthEnforce.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(getMethod(cls, "methodWithoutAuthEnforce", NamespaceId.class), rewrittenObject, DummyAuthEnforce
      .EnforceNotCalledException.class, NamespaceId.DEFAULT);

    // test that class rewriting works for a valid annotated method in another inner class and needs the
    // invokeSetters to called independently for this
    cls = classLoader.loadClass(DummyAuthEnforce.AnotherValidAuthEnforceAnnotations.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(getMethod(cls, "testSomeOtherAction", NamespaceId.class), rewrittenObject,
                ExceptionAuthorizationEnforcer.ExpectedException.class, NamespaceId.DEFAULT);

    // test that class rewriting works for a valid annotation with field instances
    cls = classLoader.loadClass(DummyAuthEnforce.ValidAuthEnforceWithFields.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(getMethod(cls, "testNoParameters"), rewrittenObject,
                ExceptionAuthorizationEnforcer.ExpectedException.class);
    testRewrite(getMethod(cls, "testParaNameSameAsField", NamespaceId.class), rewrittenObject,
                new NamespaceId("ns"), ExceptionAuthorizationEnforcer.ExpectedException.class, NamespaceId.DEFAULT);
    testRewrite(getMethod(cls, "testParaPreference", InstanceId.class), rewrittenObject,
                new InstanceId("i1"), ExceptionAuthorizationEnforcer.ExpectedException.class, new InstanceId("i1"));
    testRewrite(getMethod(cls, "testThisClassPreference", NamespaceId.class), rewrittenObject,
                new NamespaceId("ns"), ExceptionAuthorizationEnforcer.ExpectedException.class, NamespaceId.DEFAULT);
  }

  @Test
  public void testInvalidEntity() throws Exception {
    // tests that class rewrite fails if no parameters are found with a Name specified in the annotation entities field
    testInvalidEntityHelper(DummyAuthEnforce.AbsentEntityName.class);
    // tests that class rewrite fails if invalid parameters are found with a Name specified in the annotation entities
    // field
    testInvalidEntityHelper(DummyAuthEnforce.InvalidEntityName.class);
    // test that the class rewrite fails if two parameters are found with the same Name annotation
    testInvalidEntityHelper(DummyAuthEnforce.DuplicateEntityName.class);
  }

  private void testInvalidEntityHelper(Class cls) throws Exception {
    try {
      rewrite(cls);
      Assert.fail("An IllegalArgumentException should have been thrown earlier.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  private Method getMethod(Class<?> cls, String methodName, Class<?>... parameterTypes) throws NoSuchMethodException {
    return cls.getDeclaredMethod(methodName, parameterTypes);
  }

  private void testRewrite(Method method, Object rewrittenObject, EntityId entityId,
                           Class<? extends Exception> expectedException, Object... args) throws NoSuchMethodException {
    try {
      method.invoke(rewrittenObject, args);
    } catch (Exception e) {
      // Since the above method is invoked through reflection any exception thrown will be wrapped in
      // InvocationTargetException so verify that the root cause is the expected exception confirming that enforce
      // was called successfully.
      if (!(e instanceof InvocationTargetException && expectedException.isAssignableFrom(e.getCause().getClass()))) {

        Assert.fail(String.format("Got exception %s while expecting %s%s%s", e.getCause(),
                                  ExceptionAuthorizationEnforcer.ExpectedException.class.getName(),
                                  System.lineSeparator(), getFormattedStackTrace(e.getStackTrace())));
      }
      if (entityId != null) {
        if (!ExceptionAuthorizationEnforcer.ExpectedException.class.isAssignableFrom(e.getCause().getClass())) {
          Assert.fail(String.format("Exception %s is not assignable from %s to match entity %s", e.getCause(),
                                    ExceptionAuthorizationEnforcer.ExpectedException.class.getName(), entityId));

        }
        ExceptionAuthorizationEnforcer.ExpectedException exception =
          (ExceptionAuthorizationEnforcer.ExpectedException) e.getCause();
        if (!exception.getEntityId().equals(entityId)) {
          Assert.fail(String.format("Expected %s with entity %s but found %s", ExceptionAuthorizationEnforcer
            .ExpectedException.class.getSimpleName(), entityId, exception.getEntityId()));
        }
      }
    }
  }

  private void testRewrite(Method method, Object rewrittenObject, Class<? extends Exception> expectedException,
                           Object... args) throws NoSuchMethodException {
    testRewrite(method, rewrittenObject, null, expectedException, args);
  }

  private void invokeSetters(Class<?> cls, Object rewrittenObject)
    throws InvocationTargetException, IllegalAccessException {
    Method[] declaredMethods = cls.getDeclaredMethods();
    for (Method declaredMethod : declaredMethods) {
      // if the method name starts with set_ then we know its an generated setter
      if (declaredMethod.getName().startsWith(AuthEnforceRewriter.GENERATED_SETTER_METHOD_PREFIX +
                                                AuthEnforceRewriter.GENERATED_FIELD_PREFIX)) {
        declaredMethod.setAccessible(true); // since its setter it might be private
        if (declaredMethod.getName().contains(AuthEnforceRewriter.AUTHENTICATION_CONTEXT_FIELD_NAME)) {
          declaredMethod.invoke(rewrittenObject, new AuthenticationTestContext());
        } else if (declaredMethod.getName().contains(AuthEnforceRewriter.AUTHORIZATION_ENFORCER_FIELD_NAME)) {
          declaredMethod.invoke(rewrittenObject, new ExceptionAuthorizationEnforcer());
        } else {
          throw new IllegalStateException(String.format("Found an expected setter method with name %s. While trying " +
                                                          "invoke setter for %s and %s",
                                                        declaredMethod.getName(),
                                                        AuthenticationContext.class.getSimpleName(),
                                                        AuthorizationEnforcer.class.getSimpleName()));
        }
      }
    }
  }

  private ClassDefinition rewrite(Class cls) throws Exception {
    AuthEnforceRewriter rewriter = new AuthEnforceRewriter();
    URL url = cls.getClassLoader().getResource(cls.getName().replace('.', '/') + ".class");
    Assert.assertNotNull(url);
    try (InputStream is = url.openStream()) {
      return new ClassDefinition(rewriter.rewriteClass(cls.getName(), is), Type.getInternalName(cls));
    }
  }

  private Object loadRewritten(ClassLoader classLoader, String outerClassName, String innerClassName)
    throws ClassNotFoundException,
    IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
    // the classes which we are loading from DummyAuthEnforce are inner classes so we need to instantiate the outer
    // class to load them.
    Class<?> outerClass = classLoader.loadClass(outerClassName);
    Object outer = outerClass.newInstance();
    return classLoader.loadClass(innerClassName).getDeclaredConstructor(outerClass).newInstance(outer);
  }

  private String getFormattedStackTrace(StackTraceElement[] stackTraceElements) {
    StringBuilder stringBuilder = new StringBuilder();
    for (StackTraceElement stackTraceElement : stackTraceElements) {
      stringBuilder.append(stackTraceElement);
      stringBuilder.append(System.lineSeparator());
    }
    return stringBuilder.toString();
  }
}
