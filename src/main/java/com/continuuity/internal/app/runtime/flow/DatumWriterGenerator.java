package com.continuuity.internal.app.runtime.flow;

import com.continuuity.common.io.Encoder;
import com.continuuity.internal.api.io.Schema;
import com.continuuity.internal.api.io.SchemaHash;
import com.continuuity.internal.io.DatumWriter;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;
import org.objectweb.asm.signature.SignatureVisitor;
import org.objectweb.asm.signature.SignatureWriter;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
@NotThreadSafe
public final class DatumWriterGenerator {

  private final Map<String, Method> encodeMethods = Maps.newHashMap();
  private ClassWriter classWriter;
  private String className;

  static final class ClassDefinition {
    private final byte[] bytecode;
    private final String className;

    public ClassDefinition(byte[] bytecode, String className) {
      this.bytecode = bytecode;
      this.className = className;
    }

    public byte[] getBytecode() {
      return bytecode;
    }

    public String getClassName() {
      return className;
    }
  }

  public ClassDefinition generate(TypeToken<?> outputType, Schema schema) {
    classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    TypeToken<?> interfaceType = getInterfaceType(outputType);

    // Generate the class
    className = getClassName(interfaceType, schema);
    classWriter.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_FINAL,
                      className, getClassSignature(interfaceType),
                      Type.getInternalName(Object.class),
                      new String[]{Type.getInternalName(DatumWriter.class)});

    // Static schema hash field, for verification
    classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_STATIC + Opcodes.ACC_FINAL, "SCHEMA_HASH",
                           Type.getDescriptor(String.class), null, schema.getSchemaHash().toString()).visitEnd();
    // Schema field
    classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL, "schema",
                           Type.getDescriptor(Schema.class), null, null).visitEnd();
    // Constructor
    generateConstructor();
    // Encode method
    generateEncode(outputType, schema);

    // DEBUG block. Uncomment for debug
//    org.objectweb.asm.ClassReader reader = new org.objectweb.asm.ClassReader(classWriter.toByteArray());
//    reader.accept(new org.objectweb.asm.util.CheckClassAdapter(
//                    new org.objectweb.asm.util.TraceClassVisitor(new PrintWriter(System.out))), 0);
//
//    java.io.File file = new java.io.File("/tmp/" + className + ".class");
//    file.getParentFile().mkdirs();
//    System.out.println(file);
//    try {
//      ByteStreams.write(classWriter.toByteArray(), Files.newOutputStreamSupplier(file));
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
    // End DEBUG block

    return new ClassDefinition(classWriter.toByteArray(), className);
  }

  private void generateConstructor() {
    Method constructor = getMethod(void.class, "<init>", Schema.class);

    // Constructor(Schema schema)
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, constructor, null, null, classWriter);

    // super(); // Calling Object constructor
    mg.loadThis();
    mg.dup();
    mg.invokeConstructor(Type.getType(Object.class), getMethod(void.class, "<init>"));

    // if (!SCHEMA_HASH.equals(schema.getSchemaHash().toString())) { throw IllegalArgumentException }
    mg.getStatic(Type.getType(className), "SCHEMA_HASH", Type.getType(String.class));
    mg.loadArg(0);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(SchemaHash.class, "getSchemaHash"));
    mg.invokeVirtual(Type.getType(SchemaHash.class), getMethod(String.class, "toString"));
    mg.invokeVirtual(Type.getType(String.class), getMethod(boolean.class, "equals", Object.class));
    Label hashEquals = mg.newLabel();
    mg.ifZCmp(GeneratorAdapter.NE, hashEquals);
    mg.throwException(Type.getType(IllegalArgumentException.class), "Schema not match.");
    mg.mark(hashEquals);

    // this.schema = schema;
    mg.loadArg(0);
    mg.putField(Type.getType(className), "schema", Type.getType(Schema.class));

    mg.returnValue();
    mg.endMethod();
  }

  private void generateEncode(TypeToken<?> outputType, Schema schema) {
    Method encodeMethod = getMethod(void.class, "encode", outputType.getRawType(), Encoder.class);

    // Generate the synthetic method for the bridging
    Method method = getMethod(void.class, "encode", Object.class, Encoder.class);
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC + Opcodes.ACC_BRIDGE + Opcodes.ACC_SYNTHETIC,
                                               method, null, new Type[] {Type.getType(IOException.class)}, classWriter);

    mg.loadThis();
    mg.loadArg(0);
    mg.checkCast(Type.getType(outputType.getRawType()));
    mg.loadArg(1);
    mg.invokeVirtual(Type.getType(className), encodeMethod);
    mg.returnValue();
    mg.endMethod();

    // Generate the top level encode method
    String methodSignature = null;
    if (outputType.getType() instanceof ParameterizedType) {
      methodSignature = getEncodeMethodSignature(method, new TypeToken[]{outputType, null});
    }
    mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, encodeMethod, methodSignature,
                              new Type[] {Type.getType(IOException.class)}, classWriter);
    mg.loadThis();
    mg.loadArg(0);
    mg.loadArg(1);
    mg.loadThis();
    mg.getField(Type.getType(className), "schema", Type.getType(Schema.class));
    // seenRefs set
    mg.invokeStatic(Type.getType(Sets.class), getMethod(Set.class, "newIdentityHashSet"));
    mg.invokeVirtual(Type.getType(className), getEncodeMethod(outputType, schema));
    mg.returnValue();
    mg.endMethod();
  }

  private Method getEncodeMethod(TypeToken<?> outputType, Schema schema) {
    String key = String.format("%s%s", normalizeTypeName(outputType), schema.getSchemaHash());

    Method method = encodeMethods.get(key);
    if (method != null) {
      return method;
    }

    // Generate the encode method
    String methodName = String.format("encode%s", key);
    method = getMethod(void.class, methodName, outputType.getRawType(), Encoder.class, Schema.class, Set.class);

    encodeMethods.put(key, method);

    String methodSignature = getEncodeMethodSignature(method, new TypeToken[]{outputType, null, null,
                                                                              new TypeToken<Set<Object>>() {}});
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PRIVATE, method, methodSignature,
                                               new Type[]{Type.getType(IOException.class)}, classWriter);

    generateEncodeBody(mg, schema, outputType, 0, 1, 2, 3);
    mg.returnValue();
    mg.endMethod();

    return method;
  }

  /**
   * Generates the encode method body, with the binary encoder given in the local variable.
   * @param mg
   * @param schema
   */
  private void generateEncodeBody(GeneratorAdapter mg, Schema schema, TypeToken<?> outputType,
                                  int value, int encoder, int schemaLocal, int seenRefs) {
    Schema.Type schemaType = schema.getType();

    switch (schemaType) {
      case NULL:
        break;
      case BOOLEAN:
        encodeSimple(mg, outputType, "writeBool", value, encoder);
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        String encodeMethod = "write" + schemaType.name().charAt(0) + schemaType.name().substring(1).toLowerCase();
        encodeSimple(mg, outputType, encodeMethod, value, encoder);
        break;
      case ENUM:
        encodeEnum(mg, outputType, value, encoder, schemaLocal);
        break;
      case ARRAY:
        if (Collection.class.isAssignableFrom(outputType.getRawType())) {
          Preconditions.checkArgument(outputType.getType() instanceof ParameterizedType,
                                      "Only support parameterized collection type.");
          TypeToken<?> componentType = TypeToken.of(((ParameterizedType) outputType.getType())
                                                      .getActualTypeArguments()[0]);

          encodeCollection(mg, componentType, schema.getComponentSchema(), value, encoder, schemaLocal, seenRefs);
        } else if (outputType.isArray()) {
          TypeToken<?> componentType = outputType.getComponentType();
          encodeArray(mg, componentType, schema.getComponentSchema(), value, encoder, schemaLocal, seenRefs);
        }
        break;
      case MAP:
        Preconditions.checkArgument(Map.class.isAssignableFrom(outputType.getRawType()),
                                    "Only %s type is supported.", Map.class.getName());
        Preconditions.checkArgument(outputType.getType() instanceof ParameterizedType,
                                    "Only support parameterized map type.");
        java.lang.reflect.Type[] mapArgs = ((ParameterizedType) outputType.getType()).getActualTypeArguments();
        Map.Entry<Schema, Schema> mapSchema = schema.getMapSchema();
        encodeMap(mg, TypeToken.of(mapArgs[0]), TypeToken.of(mapArgs[1]),
                  mapSchema.getKey(), mapSchema.getValue(), value, encoder, schemaLocal, seenRefs);
        break;
      case RECORD:
        encodeRecord(mg, schema, outputType, value, encoder, schemaLocal, seenRefs);
        break;
      case UNION:
        encodeUnion(mg, outputType, schema, value, encoder, schemaLocal, seenRefs);
        break;
    }
  }

  /**
   * Generates method body for encoding an compile time int value.
   * @param mg
   * @param intValue
   * @param encoder
   */
  private void encodeInt(GeneratorAdapter mg, int intValue, int encoder) {
    mg.loadArg(encoder);
    mg.push(intValue);
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, "writeInt", int.class));
    mg.pop();
  }

  /**
   * Generates method body for encoding simple schema type.
   * @param mg
   * @param type
   * @param encodeMethod
   * @param value
   * @param encoder
   */
  private void encodeSimple(GeneratorAdapter mg, TypeToken<?> type,
                            String encodeMethod, int value, int encoder) {
    // encoder.writeXXX(value);
    TypeToken<?> encodeType = type;
    mg.loadArg(encoder);
    mg.loadArg(value);
    if (Primitives.isWrapperType(encodeType.getRawType())) {
      encodeType = TypeToken.of(Primitives.unwrap(encodeType.getRawType()));
      mg.unbox(Type.getType(encodeType.getRawType()));
    }
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, encodeMethod, encodeType.getRawType()));
    mg.pop();
  }

  /**
   * Generate method body for encoding enum value.
   * @param mg
   * @param outputType
   * @param value
   * @param encoder
   * @param schemaLocal
   */
  private void encodeEnum(GeneratorAdapter mg, TypeToken<?> outputType,
                          int value, int encoder, int schemaLocal) {

    // encoder.writeInt(this.schema.getEnumIndex(value.name()));
    mg.loadArg(encoder);
    mg.loadArg(schemaLocal);
    mg.loadArg(value);
    mg.invokeVirtual(Type.getType(outputType.getRawType()), getMethod(String.class, "name"));
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(int.class, "getEnumIndex", String.class));
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, "writeInt", int.class));
    mg.pop();
  }

  /**
   * Generates method body for encoding Collection value.
   * @param mg
   * @param componentType
   * @param componentSchema
   * @param value
   * @param encoder
   */
  private void encodeCollection(GeneratorAdapter mg, TypeToken<?> componentType, Schema componentSchema,
                                int value, int encoder, int schemaLocal, int seenRefs) {
    // Encode and store the collection length locally
    mg.loadArg(value);
    mg.invokeInterface(Type.getType(Collection.class), getMethod(int.class, "size"));
    int length = mg.newLocal(Type.INT_TYPE);
    mg.storeLocal(length);

    mg.loadArg(encoder);
    mg.loadLocal(length);
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, "writeInt", int.class));
    mg.pop();

    // Store the component schema
    mg.loadArg(schemaLocal);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(Schema.class, "getComponentSchema"));
    int componentSchemaLocal = mg.newLocal(Type.getType(Schema.class));
    mg.storeLocal(componentSchemaLocal);

    // Store the iterator
    int iterator = mg.newLocal(Type.getType(Iterator.class));
    mg.loadArg(value);
    mg.invokeInterface(Type.getType(Collection.class), getMethod(Iterator.class, "iterator"));
    mg.storeLocal(iterator);

    // For loop with iterator. Encode each component
    Label beginFor = mg.mark();
    Label endFor = mg.newLabel();
    mg.loadLocal(iterator);
    mg.invokeInterface(Type.getType(Iterator.class), getMethod(boolean.class, "hasNext"));
    mg.ifZCmp(GeneratorAdapter.EQ, endFor);

    mg.loadThis();
    mg.loadLocal(iterator);
    mg.invokeInterface(Type.getType(Iterator.class), getMethod(Object.class, "next"));
    mg.checkCast(Type.getType(componentType.getRawType()));
    mg.loadArg(encoder);
    mg.loadLocal(componentSchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(Type.getType(className), getEncodeMethod(componentType, componentSchema));
    mg.goTo(beginFor);

    mg.mark(endFor);

    // if length > 0, write out 0 at the end of array.
    Label zeroLength = mg.newLabel();
    mg.loadLocal(length);
    mg.ifZCmp(GeneratorAdapter.LE, zeroLength);
    encodeInt(mg, 0, encoder);
    mg.mark(zeroLength);
  }

  private void encodeArray(GeneratorAdapter mg, TypeToken<?> componentType, Schema componentSchema,
                           int value, int encoder, int schemaLocal, int seenRefs) {
    // Encode and store the array length locally
    mg.loadArg(value);
    mg.arrayLength();
    int length = mg.newLocal(Type.INT_TYPE);
    mg.storeLocal(length);

    mg.loadArg(encoder);
    mg.loadLocal(length);
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, "writeInt", int.class));
    mg.pop();

    // Store the component schema
    mg.loadArg(schemaLocal);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(Schema.class, "getComponentSchema"));
    int componentSchemaLocal = mg.newLocal(Type.getType(Schema.class));
    mg.storeLocal(componentSchemaLocal);

    // for (int idx = 0; idx < array.length; idx++)
    mg.push(0);
    int idx = mg.newLocal(Type.INT_TYPE);
    mg.storeLocal(idx);
    Label beginFor = mg.mark();
    Label endFor = mg.newLabel();
    mg.loadLocal(idx);
    mg.loadLocal(length);
    mg.ifICmp(GeneratorAdapter.GE, endFor);

    mg.loadThis();
    mg.loadArg(value);
    mg.loadLocal(idx);
    mg.arrayLoad(Type.getType(componentType.getRawType()));
    mg.loadArg(encoder);
    mg.loadLocal(componentSchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(Type.getType(className), getEncodeMethod(componentType, componentSchema));

    mg.iinc(idx, 1);
    mg.goTo(beginFor);
    mg.mark(endFor);

    // if length > 0, write out 0 at the end of array.
    Label zeroLength = mg.newLabel();
    mg.loadLocal(length);
    mg.ifZCmp(GeneratorAdapter.LE, zeroLength);
    encodeInt(mg, 0, encoder);
    mg.mark(zeroLength);
  }

  private void encodeMap(GeneratorAdapter mg, TypeToken<?> keyType, TypeToken<?> valueType, Schema keySchema,
                         Schema valueSchema, int value, int encoder, int schemaLocal, int seenRefs) {
    // Encode and store the map length locally
    mg.loadArg(value);
    mg.invokeInterface(Type.getType(Map.class), getMethod(int.class, "size"));
    int length = mg.newLocal(Type.INT_TYPE);
    mg.storeLocal(length);

    mg.loadArg(encoder);
    mg.loadLocal(length);
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, "writeInt", int.class));
    mg.pop();

    // Stores the key and value schema
    mg.loadArg(schemaLocal);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(Map.Entry.class, "getMapSchema"));
    mg.dup();

    int keySchemaLocal = mg.newLocal(Type.getType(Schema.class));
    mg.invokeInterface(Type.getType(Map.Entry.class), getMethod(Object.class, "getKey"));
    mg.checkCast(Type.getType(Schema.class));
    mg.storeLocal(keySchemaLocal);

    int valueSchemaLocal = mg.newLocal(Type.getType(Schema.class));
    mg.invokeInterface(Type.getType(Map.Entry.class), getMethod(Object.class, "getValue"));
    mg.checkCast(Type.getType(Schema.class));
    mg.storeLocal(valueSchemaLocal);

    // Store the entry set iterator
    int iterator = mg.newLocal(Type.getType(Iterator.class));
    mg.loadArg(value);
    mg.invokeInterface(Type.getType(Map.class), getMethod(Set.class, "entrySet"));
    mg.invokeInterface(Type.getType(Set.class), getMethod(Iterator.class, "iterator"));
    mg.storeLocal(iterator);

    // For loop the entry set iterator, encode each key-value pairs
    Label beginFor = mg.mark();
    Label endFor = mg.newLabel();
    mg.loadLocal(iterator);
    mg.invokeInterface(Type.getType(Iterator.class), getMethod(boolean.class, "hasNext"));
    mg.ifZCmp(GeneratorAdapter.EQ, endFor);

    int entry = mg.newLocal(Type.getType(Map.Entry.class));
    mg.loadLocal(iterator);
    mg.invokeInterface(Type.getType(Iterator.class), getMethod(Object.class, "next"));
    mg.checkCast(Type.getType(Map.Entry.class));
    mg.storeLocal(entry);

    // encode key
    mg.loadThis();
    mg.loadLocal(entry);
    mg.invokeInterface(Type.getType(Map.Entry.class), getMethod(Object.class, "getKey"));
    mg.checkCast(Type.getType(keyType.getRawType()));
    mg.loadArg(encoder);
    mg.loadLocal(keySchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(Type.getType(className), getEncodeMethod(keyType, keySchema));

    // encode value
    mg.loadThis();
    mg.loadLocal(entry);
    mg.invokeInterface(Type.getType(Map.Entry.class), getMethod(Object.class, "getValue"));
    mg.checkCast(Type.getType(valueType.getRawType()));
    mg.loadArg(encoder);
    mg.loadLocal(valueSchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(Type.getType(className), getEncodeMethod(valueType, valueSchema));

    mg.goTo(beginFor);
    mg.mark(endFor);

    // if length > 0, write out 0 at the end of map
    Label zeroLength = mg.newLabel();
    mg.loadLocal(length);
    mg.ifZCmp(GeneratorAdapter.LE, zeroLength);
    encodeInt(mg, 0, encoder);
    mg.mark(zeroLength);
  }

  private void encodeRecord(GeneratorAdapter mg, Schema schema, TypeToken<?> outputType,
                            int value, int encoder, int schemaLocal, int seenRefs) {

    try {
      boolean isInterface = outputType.getRawType().isInterface();

      /*
        Check for circular reference
        if (value != null && !seenRefs.add(value)) {
           throw new IOException(...);
        }
      */
      Label notSeen = mg.newLabel();
      mg.loadArg(value);
      mg.ifNull(notSeen);
      mg.loadArg(seenRefs);
      mg.loadArg(value);
      mg.invokeInterface(Type.getType(Set.class), getMethod(boolean.class, "add", Object.class));
      mg.ifZCmp(GeneratorAdapter.NE, notSeen);
      mg.throwException(Type.getType(IOException.class), "Circular reference not supported.");
      mg.mark(notSeen);

      // Store the list of schema fields.
      mg.loadArg(schemaLocal);
      mg.invokeVirtual(Type.getType(Schema.class), getMethod(List.class, "getFields"));
      int fieldSchemas = mg.newLocal(Type.getType(List.class));
      mg.storeLocal(fieldSchemas);

      // For each field, call the encode method for the field
      List<Schema.Field> fields = schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        Schema.Field field = fields.get(i);

        TypeToken<?> fieldType;

        // this.encodeFieldMethod(value.fieldName, encoder, fieldSchemas.get(i).getSchema());
        mg.loadThis();
        mg.loadArg(value);
        if (isInterface) {
          Method getter = getGetter(outputType, field.getName());
          fieldType = outputType.resolveType(outputType.getRawType()
                                                       .getMethod(getter.getName()).getGenericReturnType());
          mg.invokeInterface(Type.getType(outputType.getRawType()), getter);
        } else {
          fieldType = outputType.resolveType(outputType.getRawType().getField(field.getName()).getGenericType());
          mg.getField(Type.getType(outputType.getRawType()), field.getName(), Type.getType(fieldType.getRawType()));
        }
        mg.loadArg(encoder);
        mg.loadLocal(fieldSchemas);
        mg.push(i);
        mg.invokeInterface(Type.getType(List.class), getMethod(Object.class, "get", int.class));
        mg.checkCast(Type.getType(Schema.Field.class));
        mg.invokeVirtual(Type.getType(Schema.Field.class), getMethod(Schema.class, "getSchema"));
        mg.loadArg(seenRefs);
        mg.invokeVirtual(Type.getType(className), getEncodeMethod(fieldType, field.getSchema()));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  private void encodeUnion(GeneratorAdapter mg, TypeToken<?> outputType, Schema schema,
                           int value, int encoder, int schemaLocal, int seenRefs) {
    Label nullLabel = mg.newLabel();
    Label endLabel = mg.newLabel();
    mg.loadArg(value);

    mg.ifNull(nullLabel);
    // Not null, write out 0 and then encode the value
    encodeInt(mg, 0, encoder);

    mg.loadThis();
    mg.loadArg(value);
    mg.loadArg(encoder);
    mg.loadArg(schemaLocal);
    mg.push(0);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(Schema.class, "getUnionSchema", int.class));
    mg.loadArg(seenRefs);
    mg.invokeVirtual(Type.getType(className), getEncodeMethod(outputType, schema.getUnionSchema(0)));

    mg.goTo(endLabel);

    mg.mark(nullLabel);
    // Null, write out 1
    encodeInt(mg, 1, encoder);
    mg.mark(endLabel);
  }

  private <T> TypeToken<DatumWriter<T>> getInterfaceType(TypeToken<T> type) {
    return new TypeToken<DatumWriter<T>>() {
    }.where(new TypeParameter<T>() {}, type);
  }

  private String getClassName(TypeToken<?> interfaceType, Schema schema) {
    return String.format("%s/%s%s%s",
                         interfaceType.getRawType().getPackage().getName().replace('.', '/'),
                         normalizeTypeName(TypeToken.of(((ParameterizedType)interfaceType.getType())
                                                          .getActualTypeArguments()[0])),
                         interfaceType.getRawType().getSimpleName(), schema.getSchemaHash());
  }

  private String normalizeTypeName(TypeToken<?> type) {
    return type.toString().replace(".", "")
                          .replace("[]", "Array")
                          .replace("<", "Of")
                          .replace(">", "")
                          .replace(",", "To")
                          .replace(" ", "")
                          .replace("$", "");
  }

  private String getClassSignature(TypeToken<?> interfaceType) {
    SignatureWriter signWriter = new SignatureWriter();
    SignatureVisitor sv = signWriter.visitSuperclass();
    sv.visitClassType(Type.getInternalName(Object.class));
    sv.visitEnd();

    SignatureVisitor interfaceVisitor = sv.visitInterface();
    interfaceVisitor.visitClassType(Type.getInternalName(interfaceType.getRawType()));

    if (interfaceType.getType() instanceof ParameterizedType) {
      for (java.lang.reflect.Type paramType : ((ParameterizedType)interfaceType.getType()).getActualTypeArguments()) {
        interfaceVisitor.visitTypeArgument(SignatureVisitor.INSTANCEOF);
        visitTypeSignature(TypeToken.of(paramType), interfaceVisitor);
      }
    }

    sv.visitEnd();
    return signWriter.toString();
  }

  private String getEncodeMethodSignature(Method method, TypeToken<?>[] types) {
    SignatureWriter signWriter = new SignatureWriter();

    Type[] argumentTypes = method.getArgumentTypes();

    for (int i = 0; i < argumentTypes.length; i++) {
      SignatureVisitor sv = signWriter.visitParameterType();
      if (types[i] != null) {
        visitTypeSignature(types[i], sv);
      } else {
        sv.visitClassType(argumentTypes[i].getInternalName());
        sv.visitEnd();
      }
    }

    signWriter.visitReturnType().visitBaseType('V');

    return signWriter.toString();
  }

  private void visitTypeSignature(TypeToken<?> type, SignatureVisitor visitor) {
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
        visitTypeSignature(TypeToken.of(argType), visitor.visitTypeArgument(SignatureVisitor.INSTANCEOF));
      }
    }

    visitor.visitEnd();
  }

  private Method getMethod(Class<?> returnType, String name, Class<?>...args) {
    StringBuilder builder = new StringBuilder(returnType.getName())
      .append(' ').append(name).append(" (");
    Joiner.on(',').appendTo(builder, Iterators.transform(Iterators.forArray(args), new Function<Class<?>, String>() {
      @Override
      public String apply(Class<?> input) {
        if (input.isArray()) {
          return Type.getType(input.getName()).getClassName();
        }
        return input.getName();
      }
    }));
    builder.append(')');
    return Method.getMethod(builder.toString());
  }

  private Method getGetter(TypeToken<?> outputType, String fieldName) {
    Class<?> rawType = outputType.getRawType();
    try {
      return Method.getMethod(rawType.getMethod(String.format("get%c%s",
                                                              Character.toUpperCase(fieldName.charAt(0)),
                                                              fieldName.substring(1))));
    } catch (NoSuchMethodException e) {
      try {
        return Method.getMethod(rawType.getMethod(String.format("is%c%s",
                                                                Character.toUpperCase(fieldName.charAt(0)),
                                                                fieldName.substring(1))));
      } catch (NoSuchMethodException ex) {
        throw new IllegalArgumentException("Getter method not found for field " + fieldName, ex);
      }
    }
  }
}
