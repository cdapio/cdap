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
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.ClassRewriter;
import co.cask.cdap.internal.asm.Classes;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.InstanceId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedEntityId;
import co.cask.cdap.proto.id.ParentedId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import co.cask.cdap.security.spi.authorization.AuthorizationEnforcer;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.Attribute;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.TypePath;
import org.objectweb.asm.commons.AdviceAdapter;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;
import org.objectweb.asm.tree.AnnotationNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * <p>
 * A {@link ClassRewriter} for rewriting bytecode of classes which needs Authorization Enforcement and uses
 * {@link AuthEnforce} annotation.
 * </p>
 * <p>
 * This class rewriter does two passes on the class:
 * </p>
 * <p>
 * Pass 1: In first pass it visit all the classes skipping interfaces and looks for non-static methods which has
 * {@link AuthEnforce} annotation. If an {@link AuthEnforce} annotation is found then it store all the
 * {@link AuthEnforce} annotation details and also visits the parameter annotation and collects all the position of
 * parameters with {@link Name}(preferered)/{@link QueryParam}/{@link PathParam} annotation. Note: No byte code
 * modification of the class is done in this pass.
 * </p>
 * <p>
 * Pass 2: This pass only happen if a method with {@link AuthEnforce} annotation is found in the class during the
 * first pass. In this pass we rewrite the method which has {@link AuthEnforce} annotation. In the rewrite we
 * generate a call to
 * {@link AuthEnforceUtil#enforce(AuthorizationEnforcer, Object[], Class, AuthenticationContext, Set)} with
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
 *      private final AuthorizationEnforcer _[timestamp]authorizationEnforcer;
 *      private final AuthenticationContext _[timestamp]authenticationContext;
 *
 *      @Inject
 *      public void set_[timestamp]authorizationEnforcer (AuthorizationEnforcer authorizationEnforcer) {
 *        _[timestamp]authorizationEnforcer = authorizationEnforcer
 *      }
 *
 *      @Inject
 *      public void set_[timestamp]authenticationContext (AuthenticationContext authenticationContext) {
 *        _[timestamp]authenticationContext = authenticationContext
 *      }
 *
 *      public void testSingleAction(@Name("namespaceId") NamespaceId namespaceId) throws Exception {
 *        AuthEnforceUtil.enforce(_[timestamp]authorizationEnforcer, namespaceId, _[timestamp]authenticationContext,
 *                                              Set<Action.Admin>);
 *        System.out.println("Hello");
 *      }
 *     }
 *   </pre>
 * </p>
 */
public class AuthEnforceRewriter implements ClassRewriter {

  public static final String GENERATED_FIELD_PREFIX = "_";
  public static final String GENERATED_SETTER_METHOD_PREFIX = "set";
  public static final String AUTHORIZATION_ENFORCER_FIELD_NAME = AuthorizationEnforcer.class.getSimpleName();
  public static final String AUTHENTICATION_CONTEXT_FIELD_NAME = AuthenticationContext.class.getSimpleName();

  private static final Logger LOG = LoggerFactory.getLogger(AuthEnforceRewriter.class);

  private static final Type AUTHORIZATION_ENFORCER_TYPE = Type.getType(AuthorizationEnforcer.class);
  private static final Type AUTHENTICATION_CONTEXT_TYPE = Type.getType(AuthenticationContext.class);
  private static final Type ACTION_TYPE = Type.getType(Action.class);
  private static final Type AUTH_ENFORCE_UTIL_TYPE = Type.getType(AuthEnforceUtil.class);
  private static final Set<String> SUPPORTED_PARAMETER_ANNOTATIONS = Sets.newHashSet(Name.class.getName(),
                                                                                     QueryParam.class.getName(),
                                                                                     PathParam.class.getName());
  private static final Function<String, URL> resourceLookup =
    ClassLoaders.createClassResourceLookup(AuthEnforceRewriter.class.getClassLoader());
  private static final Map<String, Boolean> cache = new HashMap<>();

  @Override
  public byte[] rewriteClass(String className, InputStream input) throws IOException {
    byte[] classBytes = ByteStreams.toByteArray(input);
    // First pass: Check the class to have a method with AuthEnforce annotation if found store the annotation details
    // and parameters for the method for second pass in which class rewrite will be performed.
    ClassReader cr = new ClassReader(classBytes);

    // We inspect all EntityId class to create a graph of their parentIds
    if (Classes.isSubTypeOf(className, ParentedId.class.getName(), resourceLookup, cache)) {
      EntityIdClassVisitor classVisitor = new EntityIdClassVisitor(className);
      cr.accept(classVisitor, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);
      return classBytes;
    }

    // SKIP_CODE SKIP_DEBUG and SKIP_FRAMESto make the first pass faster since in the first pass we just want to
    // process annotations to check if the class has any method with AuthEnforce annotation. If such method is found
    // we also store the parameters which has the named annotations as specified in the entities field of the
    // AuthEnforce.
    AuthEnforceAnnotationVisitor classVisitor = new AuthEnforceAnnotationVisitor(className);
    cr.accept(classVisitor, ClassReader.SKIP_CODE | ClassReader.SKIP_DEBUG | ClassReader.SKIP_FRAMES);

    Map<Method, AnnotationDetail> methodAnnotations = classVisitor.getMethodAnnotations();
    if (methodAnnotations.isEmpty()) {
      // if no AuthEnforce annotation was found then return the original class bytes
      return classBytes;
    }
    // We found some method which has AuthEnforce annotation so we need a second pass in to rewrite the class
    // in second pass we COMPUTE_FRAMES and visit classes with EXPAND_FRAMES
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
    cr.accept(new AuthEnforceAnnotationRewriter(className, cw, classVisitor.getFieldDetails(), methodAnnotations),
            ClassReader.EXPAND_FRAMES);
    return cw.toByteArray();
  }

  private static final class EntityIdClassVisitor extends ClassVisitor {

    private final String className;

    EntityIdClassVisitor(String className) {
      super(Opcodes.ASM5);
      this.className = className;
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      super.visit(version, access, name, signature, superName, interfaces);

      if (signature == null) {
        return;
      }

      new SignatureReader(signature).accept(new SignatureVisitor(Opcodes.ASM5) {

        @Override
        public void visitClassType(String name) {
          System.out.println("### visitClassType: " + name);
          super.visitClassType(name);
        }
      });
    }
  }

  /**
   * A {@link ClassVisitor} which is used in the first pass of the {@link AuthEnforceRewriter} to detect and store
   * method with {@link AuthEnforce} annotations. Note: This is a visitor, to know the order of in which the below
   * overridden methods will be called and their overall responsibility please see {@link ClassVisitor} documentation.
   */
  private static final class AuthEnforceAnnotationVisitor extends ClassVisitor {

    private final String className;
    private final Map<Method, AnnotationDetail> methodAnnotations;
    private final Map<String, Type> fieldDetails;
    private boolean interfaceClass;

    AuthEnforceAnnotationVisitor(String className) {
      super(Opcodes.ASM5);
      this.className = className;
      this.methodAnnotations = new HashMap<>();
      this.fieldDetails = new HashMap<>();
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      interfaceClass = Modifier.isInterface(access);
    }

    @Override
    public FieldVisitor visitField(int access, final String fieldName, String desc, String signature, Object value) {
      if (interfaceClass) {
        return null;
      }
      fieldDetails.put(fieldName, Type.getType(desc));
      return super.visitField(access, fieldName, desc, signature, value);
    }

    @Override
    public MethodVisitor visitMethod(final int access, final String methodName, final String methodDesc,
                                     final String signature, final String[] exceptions) {

      // No need to visit the class to look for AuthEnforce annotation if this class is an interface or the method is
      // static.
      if (interfaceClass || Modifier.isStatic(access)) {
        return null;
      }

      final Type[] parameterTypes = Type.getArgumentTypes(methodDesc);

      // Visit the annotations of the method to determine if it has AuthEnforce annotation. If it does then collect
      // AuthEnforce annotation details and also sets a boolean flag hasEnforce which is used later in
      // visitParameterAnnotation to visit the annotations on parameters of this method only if it had AuthEnforce
      // annotation.
      return new MethodVisitor(Opcodes.ASM5) {

        final Map<Integer, ParameterDetail> parameterDetails = new HashMap<>();
        AnnotationNode authEnforceAnnotationNode;

        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {

          // If the annotation is not visible or its not AuthEnforce just skip
          if (!(visible && AuthEnforce.class.getName().equals(Type.getType(desc).getClassName()))) {
            return null;
          }
          authEnforceAnnotationNode = new AnnotationNode(desc);
          return authEnforceAnnotationNode;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
          if (!(authEnforceAnnotationNode != null && visible &&
                  (SUPPORTED_PARAMETER_ANNOTATIONS.contains(Type.getType(desc).getClassName())))) {
            return null;
          }
          // Since AuthEnforce annotation was used on this method then look for parameters with named annotation and
          // store their position.
          // decide on a precedence order.
          AnnotationNode annotationNode = new AnnotationNode(desc);
          // store the parameter position and its type with annotation detail
          ParameterDetail parameterDetail = new ParameterDetail(parameterTypes[parameter], annotationNode);
          if (parameterDetails.containsKey(parameter)) {
            // we already stored some annotation for this parameter update it if the current one is Name annotation
            // as its preferred
            if (Name.class.getName().equals(Type.getType(desc).getClassName())) {
              LOG.info("Updating parameter details for parameter at position '%s' for method '%s' in class '%s' from " +
                              "'%s' to '%s'. Since %s is preferred annotation.", parameter, methodName, className,
                      parameterDetails.get(parameter), parameterDetail);
              parameterDetails.put(parameter, parameterDetail);
            }
          } else {
            parameterDetails.put(parameter, parameterDetail);
          }
          return annotationNode;
        }

        // This is called at the end of visiting method. Here if the method has AuthEnforce annotation then we
        // process all the AnnotationNode information collected by visiting method annotation and parameter annotation.
        // store for second pass in which we will perform class rewrite
        @Override
        public void visitEnd() {
          if (authEnforceAnnotationNode == null) {
            return;
          }
          AuthEnforceAnnotationNodeProcessor nodeProcessor =
                  new AuthEnforceAnnotationNodeProcessor(authEnforceAnnotationNode);
          Map<String, Integer> paramPositions = processParameterAnnotationNode(parameterDetails);

          List<EntityPartDetail> entityPartDetails = new ArrayList<>();
          for (String name : nodeProcessor.getEntities()) {
            // just need to check for empty string since it cannot be null as that will be a compilation error
            Preconditions.checkArgument(!name.isEmpty(), "Found an empty string in entity parts of annotation %s on " +
                                          "method %s in class %s", nodeProcessor, methodName, className);
            // Make sure that the entities specified in the AuthEnforce annotation are found in method parameters
            // or class fields. Its fine if they exist at both places in that case we will give preference to
            // method parameters unless its been specified with this. in that case its always looked in class field.
            EntityPartDetail entityPart;
            // if the name starts with this we will give preference to class field since its possible to annotate a
            // method parameter with this.something
            if (name.startsWith("this.")) {
              String fieldName = getFieldName(name);
              fieldDetails.containsKey(fieldName);
              entityPart = new EntityPartDetail(fieldName, true);
            } else {
              // preference to method parameters
              if (paramPositions.containsKey(name)) {
                entityPart = new EntityPartDetail(name, false);
              } else if (fieldDetails.containsKey(name)) {
                entityPart = new EntityPartDetail(name, true);
              } else {
                // Didn't find a named method parameter or class field for the given entity name
                throw new IllegalArgumentException(String.format("No named method parameter or a class field found " +
                                "with name '%s' in class '%s' for method '%s' whereas " +
                                "it was specified in '%s' annotation", name,
                        className, methodName,
                        AuthEnforce.class.getSimpleName()));
              }
            }
            entityPartDetails.add(entityPart);
          }
          // verify that the entity parts specified in annotation is valid to do enforcement on the given enforceOn
          verifyEntityParts(entityPartDetails, nodeProcessor.getEnforceOn(), parameterDetails, paramPositions);
          // Store all the information properly for the second pass
          methodAnnotations.put(new Method(methodName, methodDesc),
                                new AnnotationDetail(entityPartDetails, nodeProcessor.getEnforceOn(),
                                                     nodeProcessor.getActions(), paramPositions));
        }

        /** Helper Methods **/

        private void verifyEntityParts(List<EntityPartDetail> entityPartDetails, Type enforceOn,
                                       Map<Integer, ParameterDetail> parameterAnnotationNode,
                                       Map<String, Integer> paramAnnotation) {
          Type entityPartType = getEntityPartType(parameterAnnotationNode, paramAnnotation, entityPartDetails.get(0));
          // TODO: Later change this to even work with parent type as we will support enforceOn parent type
          // if the first entity part is not string then it should be on same type specified in enforce on
          if (!entityPartType.equals(Type.getType(String.class))) {
            Preconditions.checkArgument(entityPartType.equals(enforceOn), "Found invalid entity type '%s' for " +
                                          "enforceOn '%s' in annotation on '%s' method in '%s' class.",
                                        entityPartType.getClassName(), enforceOn.getClassName(), methodName, className);
            Preconditions.checkArgument(getRequiredSize(enforceOn) != 0, "Invalid enforceOn %s provided.",
                                        enforceOn.getClassName());
          } else {
            // Since the entity part/parts provided is of String type so validate that we have sufficient part for
            // EntityId creation
            // verify that all parts are of type string
            for (EntityPartDetail entityPartDetail : entityPartDetails) {
              entityPartType = getEntityPartType(parameterAnnotationNode, paramAnnotation, entityPartDetail);
              Preconditions.checkArgument(entityPartType.equals(Type.getType(String.class)),
                                          "Found part %s of type %s in a multiple part entity specification of " +
                                            "AuthEnforce. Only String is supported in multiple parts.",
                                          entityPartDetail.getEntityName(), entityPartType);
            }
            int requiredSize = getRequiredSize(enforceOn);
            Preconditions.checkArgument(requiredSize != 0, "Failed to determine required number of entity parts " +
              "needed for %s. Please make sure its a valid %s class for authorization enforcement",
                                        enforceOn.getClassName(), EntityId.class.getSimpleName());
            Preconditions.checkArgument(requiredSize == entityPartDetails.size(), "Found %s entity parts in " +
                                          "AuthEnforce annotation on method %s in class %s to do enforcement on %s " +
                                          "which requires %s entity parts or an %s", entityPartDetails.size(),
                                        methodName, className, enforceOn, requiredSize, enforceOn.getClassName());

          }
        }

        private Type getEntityPartType(Map<Integer, ParameterDetail> parameterAnnotationNode,
                                       Map<String, Integer> paramAnnotation, EntityPartDetail entityPartDetail) {
          return entityPartDetail.isField() ? fieldDetails.get(entityPartDetail.getEntityName()) :
            parameterAnnotationNode.get(paramAnnotation.get(entityPartDetail.getEntityName())).getParameterType();
        }

        /**
         * Return the required size of entity parts to create the {@link EntityId} on which authorization enforcement
         * needs to be done as specified in {@link AuthEnforce#enforceOn()}
         *
         * @param enforceOn the {@link Type} of {@link EntityId} on which enforcement needs to be done
         * @return the size of entity parts needed to create the above {@link EntityId} or 0 if found an invalid
         * {@link EntityId} type.
         */
        private int getRequiredSize(Type enforceOn) {
          if (enforceOn.equals(Type.getType(InstanceId.class)) || enforceOn.equals(Type.getType(NamespaceId.class))) {
            return 1;
          } else if (enforceOn.equals(Type.getType(StreamId.class)) ||
            enforceOn.equals(Type.getType(DatasetId.class)) || enforceOn.equals(Type.getType(ApplicationId.class))) {
            return 2;
          } else if (enforceOn.equals(Type.getType(ArtifactId.class))) {
            return 3;
          } else if (enforceOn.equals(Type.getType(ProgramId.class))) {
            return 4;
          } else {
            return 0;
          }
        }
      };
    }

    Map<Method, AnnotationDetail> getMethodAnnotations() {
      return methodAnnotations;
    }

    Map<String, Type> getFieldDetails() {
      return fieldDetails;
    }
  }

  /**
   * A {@link ClassVisitor} which is used in second pass of {@link AuthEnforceRewriter} to rewrite methods which has
   * {@link AuthEnforce} annotation on it with the annotation details collected from the first pass. This is only
   * called for classes which has at least one such method. This class does byte code rewrite to generate call to
   * {@link AuthEnforceUtil#enforce(AuthorizationEnforcer, Object[], Class, AuthenticationContext, Set)}. To
   * see an example of generated byte code please see the documentation for {@link AuthEnforceRewriter}
   */
  private final class AuthEnforceAnnotationRewriter extends ClassVisitor {

    private final String className;
    private final Type classType;
    private final Map<Method, AnnotationDetail> methodAnnotations;
    private final String authenticationContextFieldName;
    private final String authorizationEnforcerFieldName;
    private final Map<String, Type> fieldDetails;

    AuthEnforceAnnotationRewriter(String className, ClassWriter cw, Map<String, Type> fieldDetails,
                                  Map<Method, AnnotationDetail> methodAnnotations) {
      super(Opcodes.ASM5, cw);
      this.className = className;
      this.classType = Type.getObjectType(className.replace(".", "/"));
      this.fieldDetails = fieldDetails;
      this.methodAnnotations = methodAnnotations;
      this.authenticationContextFieldName = generateUniqueFieldName(AUTHENTICATION_CONTEXT_FIELD_NAME);
      this.authorizationEnforcerFieldName = generateUniqueFieldName(AUTHORIZATION_ENFORCER_FIELD_NAME);
    }

    @Override
    public MethodVisitor visitMethod(int access, final String methodName, final String methodDesc, String signature,
                                     String[] exceptions) {
      MethodVisitor mv = super.visitMethod(access, methodName, methodDesc, signature, exceptions);
      // From the first pass we know the methods which has AuthEnforce annotation and needs to be rewritten
      // if this method was not identified earlier as marked with AuthEnforce annotation just skip it
      final AnnotationDetail annotationDetail = methodAnnotations.get(new Method(methodName, methodDesc));
      if (annotationDetail == null) {
        return mv;
      }
      return new AdviceAdapter(Opcodes.ASM5, mv, access, methodName, methodDesc) {
        @Override
        protected void onMethodEnter() {
          LOG.trace("AuthEnforce annotation found in class {} on method {}. Authorization enforcement command will " +
                      "be generated for Entities: {}, enforceOn: {}, actions: {}.", className, methodName,
                    annotationDetail.getEntities(), annotationDetail.getEnforceOn(), annotationDetail.getActions());

          // do class rewrite to generate the call to
          // AuthEnforceUtil#enforce(AuthorizationEnforcer, EntityId, AuthenticationContext, Set)

          // this.authorizationEnforcer
          loadThis();
          getField(classType, authorizationEnforcerFieldName, AUTHORIZATION_ENFORCER_TYPE);

          // push the parameters of method call on to the stack

          // push the entity array
          push(annotationDetail.getEntities().size());
          newArray(Type.getType(Object.class));
          for (int i = 0; i < annotationDetail.getEntities().size(); i++) {
            dup();
            push(i);
            EntityPartDetail entityPart = annotationDetail.getEntities().get(i);
            if (entityPart.isField()) {
              // load class field
              loadThis();
              getField(classType, entityPart.getEntityName(), fieldDetails.get(entityPart.getEntityName()));
            } else {
              // load method parameter
              loadArg(annotationDetail.getParameterAnnotation().get(
                      annotationDetail.getEntities().get(i).getEntityName()));
            }
            arrayStore(Type.getType(Object.class));
          }

          // push the enforceOn type
          push(annotationDetail.getEnforceOn());

          // push the authentication context
          // this.authenticationContext
          loadThis();
          getField(classType, authenticationContextFieldName, AUTHENTICATION_CONTEXT_TYPE);

          // push all the actions on to the stack
          List<Type> actionEnumSetParamTypes = new ArrayList<>();
          for (Action action : annotationDetail.getActions()) {
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

          // generate a call to AuthEnforceUtil#enforce with the above parameters on the stack
          invokeStatic(AUTH_ENFORCE_UTIL_TYPE,
                       new Method("enforce", Type.getMethodDescriptor(Type.VOID_TYPE,
                                                                      AUTHORIZATION_ENFORCER_TYPE,
                                                                      Type.getType(Object[].class),
                                                                      Type.getType(Class.class),
                                                                      AUTHENTICATION_CONTEXT_TYPE,
                                                                      Type.getType(Set.class))));
        }
      };
    }

    private String generateUniqueFieldName(String fieldName) {
      // pre-pend current system time to make it unique
      return GENERATED_FIELD_PREFIX + System.currentTimeMillis() + fieldName;
    }

    private void generateFieldAndSetter(String name, Type type) {
      String setterMethodName = GENERATED_SETTER_METHOD_PREFIX + name;
      // add the field
      super.visitField(Modifier.PRIVATE, name, type.getDescriptor(), null, null);
      // get the setter method details
      Method method = new Method(setterMethodName, Type.getMethodDescriptor(Type.VOID_TYPE, type));
      // add the setter method
      MethodVisitor methodVisitor = super.visitMethod(Modifier.PRIVATE, method.getName(), method.getDescriptor(),
                                                      null, null);
      // Put annotation on the the setter method
      AnnotationVisitor annotationVisitor = methodVisitor.visitAnnotation(Type.getType(Inject.class).getDescriptor(),
                                                                          true);
      annotationVisitor.visitEnd();

      // Code generation for the setter method
      GeneratorAdapter generatorAdapter = new GeneratorAdapter(Modifier.PRIVATE, method, methodVisitor);
      generatorAdapter.loadThis();
      // the setter method has only one argument which is what we want the field to be set to
      generatorAdapter.loadArg(0);
      generatorAdapter.putField(classType, name, type);
      generatorAdapter.returnValue();
      generatorAdapter.endMethod();
    }

    @Override
    public void visitEnd() {
      // If this class had method annotation then we need to generate the authenticationContext and
      // authorizationEnforcer field and their setters
      generateFieldAndSetter(authorizationEnforcerFieldName, AUTHORIZATION_ENFORCER_TYPE);
      generateFieldAndSetter(authenticationContextFieldName, AUTHENTICATION_CONTEXT_TYPE);
      super.visitEnd();
    }
  }

  /**
   * Process {@link AnnotationNode} information collected on a method parameters
   *
   * @param parameterAnnotationNode a {@link Map} of parameter position and annotation details of that parameter
   * @return a new {@link Map} where the key is the parameter name given in the annotation and value is the
   * position of the parameter.
   */
  private static Map<String, Integer> processParameterAnnotationNode(Map<Integer, ParameterDetail>
                                                                       parameterAnnotationNode) {
    Map<String, Integer> parameterAnnotation = new HashMap<>();
    for (Map.Entry<Integer, ParameterDetail> entry : parameterAnnotationNode.entrySet()) {
      // the AnnotationNode of parameter will be an array of size 2 where 0 is "value" and 1 is the name provided in
      // the annotation.
      String paraName = (String) entry.getValue().getAnnotationNode().values.get(1);
      int position = entry.getKey();
      // if we have already seen a parameter named with this annotation then decide the preferred parameter if possible
      if (parameterAnnotation.containsKey(paraName)) {
        position = getPreferredParameter(parameterAnnotationNode.get(parameterAnnotation.get(paraName)),
                parameterAnnotation.get(paraName), entry.getValue(), entry.getKey());

      }
      // store the parameter position with this name
      parameterAnnotation.put(paraName, position);
    }
    return parameterAnnotation;
  }

  private static int getPreferredParameter(ParameterDetail previousDetails, int prevPosition,
                                           ParameterDetail currentDetails, int currentPosition) {
    if (Name.class.getName().equals(Type.getType(previousDetails.getAnnotationNode().desc).getClassName()) ||
            Name.class.getName().equals(Type.getType(currentDetails.getAnnotationNode().desc).getClassName())) {
      // if only one of them have Name annotation then give preference to that
      if (!(Name.class.getName().equals(Type.getType(previousDetails.getAnnotationNode().desc).getClassName()) &&
              Name.class.getName().equals(Type.getType(currentDetails.getAnnotationNode().desc).getClassName()))) {
        return Name.class.getName().equals(Type.getType(previousDetails.getAnnotationNode().desc).getClassName()) ?
                prevPosition : currentPosition;
      }
    }
    throw new IllegalArgumentException(String.format("Found conflicting annotation %s and %s. If possible please " +
                    "give preference to a parameter through %s annotation or use unique names.",
            previousDetails.getAnnotationNode(), currentDetails.getAnnotationNode(), Name.class.getName()));
  }

  /**
   * A class which can process {@link AuthEnforce} {@link AnnotationNode}
   */
  private static class AuthEnforceAnnotationNodeProcessor {

    private final Set<Action> actions = new HashSet<>();
    private Type enforceOn;
    private List<String> entities;

    AuthEnforceAnnotationNodeProcessor(AnnotationNode annotationNode) {
      List values = annotationNode.values;
      // AuthEnforce AnnotationNode will have 6 values: 3 field names and another 3 field containing their values.
      Preconditions.checkArgument(values.size() == 6,
                                  "AuthEnforce annotation has three field and with their values " +
                                    "total elements should be 6 but found %s", values.size());
      for (int i = 0; i < 6; i = i + 2) { // increment by 2 to go to next field
        Object name = values.get(i);
        if ("entities".equals(name)) {
          entities = (List<String>) values.get(i + 1);
        } else if ("enforceOn".equals(name)) {
          enforceOn = (Type) values.get(i + 1);
        } else if ("actions".equals(name)) {
          // Actions is a ArrayList<String[]> where each String array is of size 2. String[0] is class name Action
          // class and String[1] is the action which was specified in the annotation
          List<String[]> actionWithClassName = (List<String[]>) values.get(i + 1);
          for (String[] action : actionWithClassName) {
            Preconditions.checkArgument(action.length == 2);
            Preconditions.checkArgument(Type.getType(action[0]).equals(Type.getType(Action.class)));
            actions.add(Action.valueOf(action[1]));
          }
        } else {
          throw new RuntimeException(String.format("Found invalid entry %s while parsing AuthEnforce details " +
                                                     "%s", name, values));
        }
      }
    }

    Type getEnforceOn() {
      return enforceOn;
    }

    List<String> getEntities() {
      return entities;
    }

    Set<Action> getActions() {
      return actions;
    }

    @Override
    public String toString() {
      return "AuthEnforceAnnotationNodeProcessor{" +
        "actions=" + actions +
        ", enforceOn=" + enforceOn +
        ", entities=" + entities +
        '}';
    }
  }

  /**
   * A wrapper to store all the annotation details of method which has {@link AuthEnforce} annotation and parameters
   * marked with {@link Name} annotation specifying the entities
   */
  private static final class AnnotationDetail {
    private final List<EntityPartDetail> entityParts;
    private final Type enforceOn;
    private final Set<Action> actions;
    private final Map<String, Integer> parameterAnnotation;

    AnnotationDetail(List<EntityPartDetail> entityParts, Type enforceOn, Set<Action> actions,
                     Map<String, Integer> parameterAnnotation) {
      this.entityParts = entityParts;
      this.enforceOn = enforceOn;
      this.actions = actions;
      this.parameterAnnotation = parameterAnnotation;
    }

    List<EntityPartDetail> getEntities() {
      return entityParts;
    }

    Type getEnforceOn() {
      return enforceOn;
    }

    Set<Action> getActions() {
      return actions;
    }

    Map<String, Integer> getParameterAnnotation() {
      return parameterAnnotation;
    }
  }

  /**
   * Wrapper class to store the entity name and whether its a class field or not
   */
  private static final class EntityPartDetail {
    private final String entityName;
    private final boolean isField;

    EntityPartDetail(String entityName, boolean isField) {
      this.entityName = entityName;
      this.isField = isField;
    }

    String getEntityName() {
      return entityName;
    }

    boolean isField() {
      return isField;
    }
  }

  /**
   * Wrapper class to store the parameter type and the annotation details for the parameter
   */
  private static final class ParameterDetail {
    private final Type parameterType;
    private final AnnotationNode annotationNode;

    ParameterDetail(Type parameterType, AnnotationNode annotationNode) {
      this.parameterType = parameterType;
      this.annotationNode = annotationNode;
    }

    Type getParameterType() {
      return parameterType;
    }

    AnnotationNode getAnnotationNode() {
      return annotationNode;
    }
  }

  /**
   * Return the field name without "this." if the given field name starts with it
   *
   * @param fieldNameWithThis the field name which starts with this.
   * @return the field name without this.
   */
  private static String getFieldName(String fieldNameWithThis) {
    Preconditions.checkArgument(fieldNameWithThis.startsWith("this."), String.format("The given field name %s does " +
                                                                                       "not start with 'this.'",
                                                                                     fieldNameWithThis));
    return fieldNameWithThis.substring(fieldNameWithThis.lastIndexOf(".") + 1);
  }
}
