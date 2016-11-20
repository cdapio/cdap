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

    // Need to invoke the method on the object created from the rewritten class in the classloader since trying to
    // cast it here to DummyAuthEnforce will fail since the object is created from a class which was loaded from a
    // different classloader.
    Class<?> cls = classLoader.loadClass(DummyAuthEnforce.ValidAuthEnforceAnnotations.class.getName());
    Object rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    // tests a valid AuthEnforce annotation which has single action
    testRewrite(cls, rewrittenObject, "testSingleAction", ExceptionAuthorizationEnforcer.ExpectedException.class);
    // tests a valid AuthEnforce annotation which has multiple action
    testRewrite(cls, rewrittenObject, "testMultipleAction", ExceptionAuthorizationEnforcer.ExpectedException.class);
    // test that the class rewrite did not affect other non annotated methods
    testRewrite(cls, rewrittenObject, "testNoAuthEnforceAnnotation", DummyAuthEnforce.EnforceNotCalledException.class);
    // test that the class rewrite works for method whose signature does not specify throws exception
    testRewrite(cls, rewrittenObject, "testMethodWithoutException",
                ExceptionAuthorizationEnforcer.ExpectedException.class);

    // tests that class rewriting does not happen if an interface has a method with AuthEnforce
    cls = classLoader.loadClass(DummyAuthEnforce.ClassImplementingInterfaceWithAuthAnnotation.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(cls, rewrittenObject, "interfaceMethodWithAuthEnforce",
                DummyAuthEnforce.EnforceNotCalledException.class);

    // test that class rewriting does not happen for classes which does not have AuthEnforce annotation on its method
    cls = classLoader.loadClass(DummyAuthEnforce.ClassWithoutAuthEnforce.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(cls, rewrittenObject, "methodWithoutAuthEnforce", DummyAuthEnforce.EnforceNotCalledException.class);

    // test that class rewriting works for a valid annotated method in another inner class and needs the
    // invokeSetters to called independently for this
    cls = classLoader.loadClass(DummyAuthEnforce.AnotherValidAuthEnforceAnnotations.class.getName());
    rewrittenObject = loadRewritten(classLoader, DummyAuthEnforce.class.getName(), cls.getName());
    invokeSetters(cls, rewrittenObject);
    testRewrite(cls, rewrittenObject, "testSomeOtherAction", ExceptionAuthorizationEnforcer.ExpectedException.class);
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

  private void testRewrite(Class<?> cls, Object rewrittenObject, String methodName,
                           Class<? extends Exception> expectedException) throws NoSuchMethodException {
    Method method = cls.getDeclaredMethod(methodName, NamespaceId.class);
    try {
      method.invoke(rewrittenObject, NamespaceId.DEFAULT);
    } catch (Exception e) {
      // Since the above method is invoked through reflection any exception thrown will be wrapped in
      // InvocationTargetException so verify that the root cause is the expected exception confirming that enforce
      // was called successfully.
      if (!(e instanceof InvocationTargetException && expectedException.isAssignableFrom(e.getCause().getClass()))) {

        Assert.fail(String.format("Got exception %s while expecting %s%s%s", e.getCause(),
                                  ExceptionAuthorizationEnforcer.ExpectedException.class.getName(),
                                  System.lineSeparator(), getFormattedStackTrace(e.getStackTrace())));
      }
    }
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
