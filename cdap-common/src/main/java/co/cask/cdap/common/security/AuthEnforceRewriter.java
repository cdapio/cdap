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

package co.cask.cdap.common.security;

import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.common.lang.ClassRewriter;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>
 * A {@link ClassRewriter} for rewriting bytecode of classes which needs Authorization Enforcement and uses
 * {@link AuthEnforce} annotation.
 * </p>
 * <p>
 * This class rewriter does two pass on the class:
 * </p>
 * <p>
 * Pass 1: In first pass it visit all the classes skipping interfaces and looks for non-static methods which has
 * {@link AuthEnforce} annotation. If an {@link AuthEnforce} annotation is found then it store all the
 * {@link AuthEnforce} annotation details and also visits the parameter annotation and collects all the position of
 * parameters with {@link Name} annotation. Note: No byte code modification of the class is done in this pass.
 * </p>
 * <p>
 * Pass 2: This pass only happen if a method with {@link AuthEnforce} annotation is found in the class during the
 * first pass. In this pass we rewrite the method which has {@link AuthEnforce} annotation. In the rewrite we
 * generate a call to
 * {@link AuthEnforceAnnotationEnforcer#enforce(AuthorizationEnforcer, EntityId, AuthenticationContext, Set)} with
 * all the annotation details collected in the first pass.
 * </p>
 * <p>
 * Below is a sample of how a class containing a method annotation with {@link AuthEnforce} looks like before and
 * after class rewrite.
 * </p>
 * <p>
 * Before:
 * <pre>
 *     public class ValidAuthEnforceAnnotations {
 *      private final AuthorizationEnforcer authorizationEnforcer;
 *      private final AuthenticationContext authenticationContext;
 *
 *      public ValidAuthEnforceAnnotations() {
 *        authenticationContext = new AuthenticationTestContext();
 *        authorizationEnforcer = new ExceptionAuthorizationEnforcer();
 *      }
 *
 *      &#064;AuthEnforce(entities = "namespaceId", enforceOn = NamespaceId.class, actions = Action.ADMIN)
 *      public void testSingleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
 *        System.out.println("Hello");
 *      }
 *     }
 *   </pre>
 * </p>
 * <p>
 * After:
 * <pre>
 *     public class ValidAuthEnforceAnnotations {
 *      private final AuthorizationEnforcer authorizationEnforcer;
 *      private final AuthenticationContext authenticationContext;
 *
 *      public ValidAuthEnforceAnnotations() {
 *        authenticationContext = new AuthenticationTestContext();
 *        authorizationEnforcer = new ExceptionAuthorizationEnforcer();
 *      }
 *
 *      public void testSingleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
 *        AuthEnforceAnnotationEnforcer.enforce(this.authorizationEnforcer, namespaceId, authenticationContext,
 *                                              Set<Action.Admin>);
 *        System.out.println("Hello");
 *      }
 *     }
 *   </pre>
 * </p>
 */
public class AuthEnforceRewriter implements ClassRewriter {

  private static final Logger LOG = LoggerFactory.getLogger(AuthEnforceRewriter.class);

  private static final Type AUTHORIZATION_ENFORCER_TYPE = Type.getType(AuthorizationEnforcer.class);
  private static final Type AUTHENTICATION_CONTEXT_TYPE = Type.getType(AuthenticationContext.class);
  private static final Type ACTION_TYPE = Type.getType(Action.class);
  private static final Type AUTH_ENFORCE_ANNOTATION_ENFORCER_TYPE = Type.getType(AuthEnforceAnnotationEnforcer.class);
  private Map<Method, AnnotationDetails> methodAnnotations;

  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    byte[] classBytes = ByteStreams.toByteArray(input);
    methodAnnotations = new HashMap<>(); // re-initialize for this new class.

    // First pass: Check the class to have a method with AuthEnforce annotation if found store the annotation details
    // and parameters for the method for second pass in which class rewrite will be performed.
    ClassReader cr = new ClassReader(classBytes);

    // 0 because we don't want the class writer to compute frames or maxs. Since
    // in first pass we just pass the classes for annotation. // Note: There is no need to compute frames in this
    // pass since we are not generating any byte code and for classes which does not have AuthEnforce annotation we
    // just return original byte[] from the passed input stream. Trying to compute frames require loading all the
    // dependent classes for a class and this might lead to class loading problems if some dependent classes are not
    // available. For example HbaseTableUtil use Hbase classes and in in-memory and standalone cdap hbase classes
    // are not present.
    ClassWriter cw = new ClassWriter(0);

    // SKIP_CODE to make the first pass faster since in the first pass we just want to process annotations to check
    // if the class has any method with AuthEnforce annotation. If such method is found we also store the parameters
    // which has the named annotations as specified in the entities field of the AuthEnforce.
    cr.accept(new AuthEnforceAnnotationVisitor(className, cw), ClassReader.SKIP_CODE);

    if (!methodAnnotations.isEmpty()) {
      // We found some method which has AuthEnforce annotation so we need a second pass in to rewrite the class
      cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES); // in second pass we COMPUTE_FRAMES and visit classes with
      // EXPAND_FRAMES
      cr.accept(new AuthEnforceAnnotationRewriter(className, cw), ClassReader.EXPAND_FRAMES);
      return cw.toByteArray();
    }
    // if no AuthEnforce annotation was found then return the original class bytes
    return classBytes;
  }


  /**
   * A {@link ClassVisitor} which is used in the first pass of the {@link AuthEnforceRewriter} to detect and store
   * method with {@link AuthEnforce} annotations. Note: This is a visitor, to know the order of in which the below
   * overridden methods will be called and their overall responsibility please see {@link ClassVisitor} documentation.
   */
  private final class AuthEnforceAnnotationVisitor extends ClassVisitor {

    private final String className;
    private boolean interfaceClass;

    AuthEnforceAnnotationVisitor(String className, ClassWriter cw) {
      super(Opcodes.ASM5, cw);
      this.className = className;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      // visit the header of the class to identify if this class is an interface
      super.visit(version, access, name, signature, superName, interfaces);
      interfaceClass = Modifier.isInterface(access);
    }

    @Override
    public MethodVisitor visitMethod(final int access, final String methodName, final String methodDesc,
                                     final String signature, final String[] exceptions) {
      MethodVisitor mv = super.visitMethod(access, methodName, methodDesc, signature, exceptions);

      // No need to visit the class to look for AuthEnforce annotation if this class is an interface or the method is
      // static.
      if (interfaceClass || Modifier.isStatic(access)) {
        return mv;
      }

      final boolean[] hasAuthEnforce = new boolean[1];
      final AuthEnforce[] authEnforceAnnotation = new AuthEnforce[1];
      final Map<String, Integer> paramAnnotation = new HashMap<>();

      // Visit the annotations of the method to determine if it has AuthEnforce annotation. If it does then collect
      // all the AuthEnforce annotation details through AnnotationVisitor and also sets a boolean flag hasEnforce
      // which is used later in visitParameterAnnotation to visit the annotations on parameters of this method only
      // if it had AuthEnforce annotation.
      mv = new MethodVisitor(Opcodes.ASM5, mv) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          AnnotationVisitor annotationVisitor = super.visitAnnotation(desc, visible);
          if (visible) {
            // if the annotation is present then visit the annotation
            if (AuthEnforce.class.getName().equals(Type.getType(desc).getClassName())) {
              // set the flag which will be used to decide whether to visit parameter annotation or not
              hasAuthEnforce[0] = true;
              final Class[] enforceOn = new Class[1];
              final List<String> entities = new ArrayList<>();
              final Set<Action> actions = new HashSet<>();

              // We found AuthEnforce annotation so visit the annotation to collect all the specified details
              // See AnnotationVisitor documentation to know the order in which the below overridden methods are
              // called and their responsibilities.
              annotationVisitor = new AnnotationVisitor(Opcodes.ASM5, annotationVisitor) {
                // gets the entity id class on which enforcement has to performed.
                @Override
                public void visit(String name, Object value) {
                  try {
                    enforceOn[0] = Class.forName(((Type) value).getClassName());
                  } catch (ClassNotFoundException e) {
                    Throwables.propagate(e);
                  }
                  super.visit(name, value);
                }

                // gets the entity parts and actions for enforcement by visiting the array for values
                @Override
                public AnnotationVisitor visitArray(String name) {
                  AnnotationVisitor av = super.visitArray(name);
                  return new AnnotationVisitor(Opcodes.ASM5, av) {
                    // get the entity parts on which enforcement needs to be performed
                    @Override
                    public void visit(String name, Object value) {
                      entities.add((String) value);
                      super.visit(name, value);
                    }

                    //  get the actions which needs to be checked during enforcement
                    @Override
                    public void visitEnum(String name, String desc, String value) {
                      actions.add(Action.valueOf(value));
                      super.visitEnum(name, desc, value);
                    }
                  };
                }
              };
              authEnforceAnnotation[0] = new AuthEnforceAnnotation().getInstance(entities, enforceOn[0], actions);
            }
          }
          return annotationVisitor;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(final int parameter, String desc, boolean visible) {
          AnnotationVisitor annotationVisitor = super.visitParameterAnnotation(parameter, desc, visible);
          // If AuthEnforce annotation was used on this method then look for parameters with named annotation and
          // store their position.
          // TODO: Support looking class field too in the ClassVisitor
          // TODO: Should also pick up names from QueryParam and PathParam annotations here as needed later
          if (hasAuthEnforce[0] && visible && Name.class.getName().equals(Type.getType(desc).getClassName())) {
            annotationVisitor = new AnnotationVisitor(Opcodes.ASM5, annotationVisitor) {
              @Override
              public void visit(String name, Object value) {
                // we expect the parameters to have unique name
                Preconditions.checkArgument(!paramAnnotation.containsKey(value),
                                            String.format("A parameter with name %s was already found at position %s." +
                                                            " Please use unique names.", value,
                                                          paramAnnotation.get(value)));
                paramAnnotation.put((String) value, parameter);
                super.visit(name, value);
              }
            };
          }
          return annotationVisitor;
        }

        // This is called at the end of visiting method. Here if the method has AuthEnforce annotation then we store
        // all the annotation details in with the method details for second pass in which we will perform class
        // rewrite
        @Override
        public void visitEnd() {
          if (hasAuthEnforce[0]) {
            // verify that we found all the values specified in entities
            for (String name : authEnforceAnnotation[0].entities()) {
              Preconditions.checkArgument(paramAnnotation.containsKey(name),
                                          String.format("No method parameter found wih a name %s for method %s in " +
                                                          "class %s whereas it was specified in AuthEnforce " +
                                                          "annotation.", name, methodName, className));
            }
            // Store all the information for the second pass
            methodAnnotations.put(new Method(methodName, methodDesc),
                                  new AnnotationDetails(authEnforceAnnotation[0], paramAnnotation));
          }
          super.visitEnd();
        }
      };
      return mv;
    }
  }

  /**
   * A {@link ClassVisitor} which is used in second pass of {@link AuthEnforceRewriter} to rewrite methods which has
   * {@link AuthEnforce} annotation on it with the annotation details collected from the first pass. This is only
   * called for classes which has at least one such method. This class does byte code rewrite to generate call to
   * {@link AuthEnforceAnnotationEnforcer#enforce(AuthorizationEnforcer, EntityId, AuthenticationContext, Set)}. To
   * see an example of generated byte code please see the documentation for {@link AuthEnforceRewriter}
   */
  private final class AuthEnforceAnnotationRewriter extends ClassVisitor {

    private final String className;
    private final Type classType;

    AuthEnforceAnnotationRewriter(String className, ClassWriter cw) {
      super(Opcodes.ASM5, cw);
      this.className = className;
      this.classType = Type.getObjectType(className.replace(".", "/"));
    }

    @Override
    public MethodVisitor visitMethod(int access, final String methodName, final String methodDesc, String signature,
                                     String[] exceptions) {
      MethodVisitor mv = super.visitMethod(access, methodName, methodDesc, signature, exceptions);
      // From the first pass we know the methods which has AuthEnforce annotation and needs to be rewritten
      if (methodAnnotations.containsKey(new Method(methodName, methodDesc))) {
        return new AdviceAdapter(Opcodes.ASM5, mv, access, methodName, methodDesc) {
          @Override
          protected void onMethodEnter() {
            // Get the annotation details which was collected by visiting annotations during the first pass
            AnnotationDetails annotationDetails = methodAnnotations.get(new Method(methodName, methodDesc));

            LOG.debug("AuthEnforce annotation found in class {} on method {}. Authorization enforcement command will " +
                        "be generated for Entities: {}, enforceOn: {}, actions: {}.", className, methodName,
                      annotationDetails.getAnnotation().entities(), annotationDetails.getAnnotation().enforceOn(),
                      annotationDetails.getAnnotation().actions());

            // TODO: Remove this one we support AuthEnforce with multiple string parts
            Preconditions.checkArgument(annotationDetails.getAnnotation().entities().length == 1,
                                        "Currently Authorization annotation is only supported for EntityId from " +
                                          "method parameter.");

            // do class rewrite to generate the call to
            // AuthEnforceAnnotationEnforcer#enforce(AuthorizationEnforcer, EntityId, AuthenticationContext, Set)

            // TODO AuthorizationEnforcer field should be generated in the class containing annotation through class
            // rewrite itself. Support this here too.
            // this.authorizationEnforcer
            loadThis();
            getField(classType, "authorizationEnforcer", AUTHORIZATION_ENFORCER_TYPE);

            // push the parameters of method call on to the stack

            // pushed the entity id
            visitVarInsn(ALOAD, annotationDetails.getParameterAnnotation()
              .get(annotationDetails.getAnnotation().entities()[0]) + 1); // + 1 because 0 specify "this"

            // TODO Similar to AuthorizationEnforcer AuthenticationContext field should be generated through class
            // rewrite. Support this too.
            // push the authentication context
            // this.authenticationContext
            loadThis();
            getField(classType, "authenticationContext", AUTHENTICATION_CONTEXT_TYPE);

            // push all the actions on to the stack
            List<Type> actionEnumSetParamTypes = new ArrayList<>();
            for (Action action : annotationDetails.getAnnotation().actions()) {
              getStatic(ACTION_TYPE, action.name().toUpperCase(), ACTION_TYPE);
              // store the Type of Enum for all the action pushed on stack as it will later be used to generate a
              // method call instruction
              actionEnumSetParamTypes.add(Type.getType(Enum.class));
            }

            // create a EnumSet from the above actions
            invokeStatic(Type.getType(EnumSet.class),
                         new Method("of", Type.getMethodDescriptor(Type.getType(EnumSet.class),
                                                                   actionEnumSetParamTypes.toArray(
                                                                     new Type[actionEnumSetParamTypes.size()]))));

            // generate a call to AuthEnforceAnnotationEnforcer#enforce with the above parameters on the stack
            invokeStatic(AUTH_ENFORCE_ANNOTATION_ENFORCER_TYPE,
                         new Method("enforce", Type.getMethodDescriptor(Type.VOID_TYPE,
                                                                        AUTHORIZATION_ENFORCER_TYPE,
                                                                        Type.getType(EntityId.class),
                                                                        AUTHENTICATION_CONTEXT_TYPE,
                                                                        Type.getType(Set.class))));
          }
        };
      }
      return mv;
    }
  }

  /**
   * Class to store all the information specified in {@link AuthEnforce} on a method
   */
  private class AuthEnforceAnnotation {

    AuthEnforce getInstance(final List<String> entities, final Class enforceOn, final Set<Action> actions) {

      return new AuthEnforce() {
        @Override
        public Class<? extends Annotation> annotationType() {
          return AuthEnforce.class;
        }

        @Override
        public String[] entities() {
          return entities.toArray(new String[entities.size()]);
        }

        @Override
        public Class enforceOn() {
          return enforceOn;
        }

        @Override
        public Action[] actions() {
          return actions.toArray(new Action[actions.size()]);
        }
      };
    }
  }

  /**
   * A wrapper to store all the annotation details of method which has {@link AuthEnforce} annotation and parameters
   * marked with {@link Name} annotation specifying the entities
   */
  private class AnnotationDetails {
    final AuthEnforce annotation;
    final Map<String, Integer> parameterAnnotation;

    AnnotationDetails(AuthEnforce annotation, Map<String, Integer> parameterAnnotation) {
      this.annotation = annotation;
      this.parameterAnnotation = parameterAnnotation;
    }

    public AuthEnforce getAnnotation() {
      return annotation;
    }

    Map<String, Integer> getParameterAnnotation() {
      return parameterAnnotation;
    }
  }
}
