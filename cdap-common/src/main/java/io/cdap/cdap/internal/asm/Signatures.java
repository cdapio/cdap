/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.internal.asm;

import com.google.common.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.signature.SignatureVisitor;
import org.objectweb.asm.signature.SignatureWriter;

/**
 * Util class for generating class signature.
 */
public final class Signatures {

  public static String getClassSignature(TypeToken<?> interfaceType) {
    SignatureWriter signWriter = new SignatureWriter();
    SignatureVisitor sv = signWriter.visitSuperclass();
    sv.visitClassType(Type.getInternalName(Object.class));
    sv.visitEnd();

    SignatureVisitor interfaceVisitor = sv.visitInterface();
    interfaceVisitor.visitClassType(Type.getInternalName(interfaceType.getRawType()));

    if (interfaceType.getType() instanceof ParameterizedType) {
      for (java.lang.reflect.Type paramType : ((ParameterizedType) interfaceType.getType()).getActualTypeArguments()) {
        interfaceVisitor.visitTypeArgument(SignatureVisitor.INSTANCEOF);
        visitTypeSignature(interfaceType.resolveType(paramType), interfaceVisitor);
      }
    }

    sv.visitEnd();
    return signWriter.toString();
  }

  /**
   * Generates signature for the given method.
   *
   * @param method Method that needs signature to be generated
   * @param returnType the {@link TypeToken} of the return type
   * @param argTypes List of {@link TypeToken} that matches with the number of arguments in the
   *     given method. For a given method argument, if the corresponding {@link TypeToken} is
   *     non-null, it will be used to generate the type parameter in the signature, otherwise, the
   *     one in the method will be used. It's useful if the method is parameterized so that
   *     parameterize type information can be passed through the type tokens.
   * @return A method signature string
   */
  public static String getMethodSignature(Method method, TypeToken<?> returnType,
      TypeToken<?>... argTypes) {
    SignatureWriter signWriter = new SignatureWriter();

    Type[] argumentTypes = method.getArgumentTypes();

    for (int i = 0; i < argumentTypes.length; i++) {
      SignatureVisitor sv = signWriter.visitParameterType();
      if (argTypes[i] != null) {
        visitTypeSignature(argTypes[i], sv);
      } else {
        sv.visitClassType(argumentTypes[i].getInternalName());
        sv.visitEnd();
      }
    }

    visitTypeSignature(returnType, signWriter.visitReturnType());
    return signWriter.toString();
  }


  public static String getFieldSignature(TypeToken<?> fieldType) {
    SignatureWriter signWriter = new SignatureWriter();
    signWriter.visitClassType(Type.getInternalName(fieldType.getRawType()));

    if (fieldType.getType() instanceof ParameterizedType) {
      for (java.lang.reflect.Type paramType : ((ParameterizedType) fieldType.getType()).getActualTypeArguments()) {
        signWriter.visitTypeArgument(SignatureVisitor.INSTANCEOF);
        visitTypeSignature(fieldType.resolveType(paramType), signWriter);
      }
    }
    signWriter.visitEnd();
    return signWriter.toString();
  }

  public static void visitTypeSignature(TypeToken<?> type, SignatureVisitor visitor) {
    Class<?> rawType = type.getRawType();
    if (rawType.isPrimitive()) {
      visitor.visitBaseType(Type.getType(rawType).toString().charAt(0));
      return;
    } else if (rawType.isArray()) {
      visitTypeSignature(type.getComponentType(), visitor.visitArrayType());
      return;
    } else {
      visitor.visitClassType(Type.getInternalName(rawType));
    }

    java.lang.reflect.Type visitType = type.getType();
    if (visitType instanceof ParameterizedType) {
      for (java.lang.reflect.Type argType : ((ParameterizedType) visitType).getActualTypeArguments()) {
        visitTypeSignature(TypeToken.of(argType),
            visitor.visitTypeArgument(SignatureVisitor.INSTANCEOF));
      }
    }

    visitor.visitEnd();
  }

  private Signatures() {
  }
}
