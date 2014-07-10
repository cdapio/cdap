package com.continuuity.internal.io;

import com.continuuity.common.io.Encoder;
import com.continuuity.internal.asm.ClassDefinition;
import com.continuuity.internal.asm.Methods;
import com.continuuity.internal.asm.Signatures;
import com.continuuity.internal.lang.Fields;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;
import org.objectweb.asm.commons.Method;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Class for generating {@link DatumWriter} bytecodes using ASM. The class generated will have a skeleton looks like
 * the following:
 * <pre>
 * {@code
 *
 *  public final class generatedClassName implements DatumWriter<OutputType> {
 *    private static final String SCHEMA_HASH = "schema_hash_as_hex_string";
 *    private final Schema schema;
 *
 *    public generatedClassName(Schema schema) {
 *      if (!SCHEMA_HASH.equals(schema.getSchemaHash().toString())) {
 *        throw new IllegalArgumentException("Schema not match.");
 *      }
 *      this.schema = schema;
 *    }
 *
 *    @Override
 *    public void encode(OutputType data, Encoder encoder) throws IOException {
 *      generatedEncodeMethod(data, encode, Sets.<Object>newIdentityHashSet());
 *    }
 *
 *    private void generatedEncodeMethod(OutputType data, Encoder encoder) {
 *      // Do actual encoding by calling methods on encoder based on the type.
 *    }
 *
 *    // Could have more generatedEncodeMethods...
 *  }
 * }
 * </pre>
 *
 * For example, to encode type int[], a generated {@link DatumWriter} will looks like this after decompile.
 * <pre>
 * {@code
 *
 *   public final class intArrayDatumWriter07D4F780E3528DB8C539EE5C21FDDEAE implements DatumWriter<int[]> {
 *     private static final String SCHEMA_HASH = "07D4F780E3528DB8C539EE5C21FDDEAE";
 *     private final Schema schema;
 *
 *     public intArrayDatumWriter07D4F780E3528DB8C539EE5C21FDDEAE(Schema paramSchema) {
 *       if (!SCHEMA_HASH.equals(paramSchema.getSchemaHash().toString())) {
 *         throw new IllegalArgumentException("Schema not match.");
 *       }
 *       this.schema = paramSchema;
 *     }
 *
 *     public void encode(int[] paramArrayOfInt, Encoder paramEncoder) throws IOException {
 *       encodeintArray07D4F780E3528DB8C539EE5C21FDDEAE(paramArrayOfInt, paramEncoder, this.schema,
 *                                                      Sets.newIdentityHashSet());
 *     }
 *
 *     private void encodeintArray07D4F780E3528DB8C539EE5C21FDDEAE(int[] paramArrayOfInt, Encoder paramEncoder,
 *                                                                 Schema paramSchema, Set<Object> paramSet)
 *                                                                 throws IOException {
 *       int i = paramArrayOfInt.length;
 *       paramEncoder.writeInt(i);
 *       Schema localSchema = paramSchema.getComponentSchema();
 *       for (int j = 0; j < i; j++) {
 *         encodeint9E688C58A5487B8EAF69C9E1005AD0BF(paramArrayOfInt[j], paramEncoder, localSchema, paramSet);
 *       }
 *       if (i > 0) {
 *         paramEncoder.writeInt(0);
 *       }
 *     }
 *
 *     private void encodeint9E688C58A5487B8EAF69C9E1005AD0BF(int paramInt, Encoder paramEncoder,
 *                                                            Schema paramSchema, Set<Object> paramSet)
 *                                                            throws IOException {
 *       paramEncoder.writeInt(paramInt);
 *     }
 *   }
 * }
 * </pre>
 *
 */
@NotThreadSafe
final class DatumWriterGenerator {

  private final Map<String, Method> encodeMethods = Maps.newHashMap();
  private final Multimap<TypeToken<?>, String> fieldAccessorRequests = HashMultimap.create();
  private ClassWriter classWriter;
  private Type classType;
//  private String className;

  /**
   * Generates a {@link DatumWriter} class for encoding data of the given output type with the given schema.
   * @param outputType Type information of the output data type.
   * @param schema Schema of the output data type.
   * @return A {@link com.continuuity.internal.asm.ClassDefinition} that contains generated class information.
   */
  ClassDefinition generate(TypeToken<?> outputType, Schema schema) {
    classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES);

    TypeToken<?> interfaceType = getInterfaceType(outputType);

    // Generate the class
    String className = getClassName(interfaceType, schema);
    classType = Type.getObjectType(className);
    classWriter.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC + Opcodes.ACC_FINAL,
                      className, Signatures.getClassSignature(interfaceType),
                      Type.getInternalName(Object.class),
                      new String[]{Type.getInternalName(interfaceType.getRawType())});

    // Static schema hash field, for verification
    classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_STATIC + Opcodes.ACC_FINAL, "SCHEMA_HASH",
                           Type.getDescriptor(String.class), null, schema.getSchemaHash().toString()).visitEnd();

    // Schema field
    classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL, "schema",
                           Type.getDescriptor(Schema.class), null, null).visitEnd();

    // Encode method
    generateEncode(outputType, schema);

    // Constructor
    generateConstructor();

    ClassDefinition classDefinition = new ClassDefinition(classWriter.toByteArray(), className);
    // DEBUG block. Uncomment for debug
//    com.continuuity.internal.asm.Debugs.debugByteCode(classDefinition, new java.io.PrintWriter(System.out));
    // End DEBUG block
    return classDefinition;
  }

  /**
   * Generates the constructor. The constructor generated has signature {@code (Schema, FieldAccessorFactory)}.
   */
  private void generateConstructor() {
    Method constructor = getMethod(void.class, "<init>", Schema.class, FieldAccessorFactory.class);

    // Constructor(Schema schema, FieldAccessorFactory accessorFactory)
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, constructor, null, null, classWriter);

    // super(); // Calling Object constructor
    mg.loadThis();
    mg.invokeConstructor(Type.getType(Object.class), getMethod(void.class, "<init>"));

    // if (!SCHEMA_HASH.equals(schema.getSchemaHash().toString())) { throw IllegalArgumentException }
    mg.getStatic(classType, "SCHEMA_HASH", Type.getType(String.class));
    mg.loadArg(0);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(SchemaHash.class, "getSchemaHash"));
    mg.invokeVirtual(Type.getType(SchemaHash.class), getMethod(String.class, "toString"));
    mg.invokeVirtual(Type.getType(String.class), getMethod(boolean.class, "equals", Object.class));
    Label hashEquals = mg.newLabel();
    mg.ifZCmp(GeneratorAdapter.NE, hashEquals);
    mg.throwException(Type.getType(IllegalArgumentException.class), "Schema not match.");
    mg.mark(hashEquals);

    // this.schema = schema;
    mg.loadThis();
    mg.loadArg(0);
    mg.putField(classType, "schema", Type.getType(Schema.class));

    // For each record field that needs an accessor, get the accessor and store it in field.
    for (Map.Entry<TypeToken<?>, String> entry : fieldAccessorRequests.entries()) {
      String fieldAccessorName = getFieldAccessorName(entry.getKey(), entry.getValue());

      classWriter.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_FINAL,
                             fieldAccessorName,
                             Type.getDescriptor(FieldAccessor.class), null, null);
      // this.fieldAccessorName
      //  = accessorFactory.getFieldAccessor(TypeToken.of(Class.forName("className")), "fieldName");
      mg.loadThis();
      mg.loadArg(1);
      mg.push(entry.getKey().getRawType().getName());
      mg.invokeStatic(Type.getType(Class.class), getMethod(Class.class, "forName", String.class));
      mg.invokeStatic(Type.getType(TypeToken.class), getMethod(TypeToken.class, "of", Class.class));
      mg.push(entry.getValue());
      mg.invokeInterface(Type.getType(FieldAccessorFactory.class),
                         getMethod(FieldAccessor.class, "getFieldAccessor", TypeToken.class, String.class));
      mg.putField(classType, fieldAccessorName, Type.getType(FieldAccessor.class));
    }

    mg.returnValue();
    mg.endMethod();
  }

  /**
   * Generates the {@link DatumWriter#encode(Object, com.continuuity.common.io.Encoder)} method.
   * @param outputType Type information of the data type for output
   * @param schema Schema to use for output.
   */
  private void generateEncode(TypeToken<?> outputType, Schema schema) {
    TypeToken<?> callOutputType = getCallTypeToken(outputType, schema);

    Method encodeMethod = getMethod(void.class, "encode", callOutputType.getRawType(), Encoder.class);

    if (!Object.class.equals(callOutputType.getRawType())) {
      // Generate the synthetic method for the bridging
      Method method = getMethod(void.class, "encode", Object.class, Encoder.class);
      GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC + Opcodes.ACC_BRIDGE + Opcodes.ACC_SYNTHETIC,
                                                 method, null, new Type[] {Type.getType(IOException.class)},
                                                 classWriter);

      mg.loadThis();
      mg.loadArg(0);
      mg.checkCast(Type.getType(callOutputType.getRawType()));
      mg.loadArg(1);
      mg.invokeVirtual(classType, encodeMethod);
      mg.returnValue();
      mg.endMethod();
    }

    // Generate the top level public encode method
    String methodSignature = null;
    if (callOutputType.getType() instanceof ParameterizedType) {
      methodSignature = Signatures.getMethodSignature(encodeMethod, new TypeToken[]{callOutputType, null});
    }
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PUBLIC, encodeMethod, methodSignature,
                              new Type[] {Type.getType(IOException.class)}, classWriter);

    // Delegate to the actual encode method(value, encoder, schema, Sets.newIdentityHashSet());
    mg.loadThis();
    mg.loadArg(0);
    mg.loadArg(1);
    mg.loadThis();
    mg.getField(classType, "schema", Type.getType(Schema.class));
    // seenRefs Set
    mg.invokeStatic(Type.getType(Sets.class), getMethod(Set.class, "newIdentityHashSet"));
    mg.invokeVirtual(classType, getEncodeMethod(outputType, schema));
    mg.returnValue();
    mg.endMethod();
  }

  /**
   * Returns the encode method for the given type and schema. The same method will be returned if the same
   * type and schema has been passed to the method before.
   *
   * @param outputType Type information of the data type for output
   * @param schema Schema to use for output.
   * @return A method for encoding the given output type and schema.
   */
  private Method getEncodeMethod(TypeToken<?> outputType, Schema schema) {
    String key = String.format("%s%s", normalizeTypeName(outputType), schema.getSchemaHash());

    Method method = encodeMethods.get(key);
    if (method != null) {
      return method;
    }

    // Generate the encode method (value, encoder, schema, set)
    TypeToken<?> callOutputType = getCallTypeToken(outputType, schema);
    String methodName = String.format("encode%s", key);
    method = getMethod(void.class, methodName, callOutputType.getRawType(),
                       Encoder.class, Schema.class, Set.class);

    // Put the method into map first before generating the body in order to support recursive data type.
    encodeMethods.put(key, method);

    String methodSignature = Signatures.getMethodSignature(method, new TypeToken[]{ callOutputType, null, null,
                                                                              new TypeToken<Set<Object>>() { }});
    GeneratorAdapter mg = new GeneratorAdapter(Opcodes.ACC_PRIVATE, method, methodSignature,
                                               new Type[]{Type.getType(IOException.class)}, classWriter);

    generateEncodeBody(mg, schema, outputType, 0, 1, 2, 3);
    mg.returnValue();
    mg.endMethod();

    return method;
  }

  /**
   * Generates the encode method body, with the binary encoder given in the local variable.
   * @param mg Method generator for generating method code body
   * @param schema Schema of the data to be encoded.
   */
  private void generateEncodeBody(GeneratorAdapter mg, Schema schema, TypeToken<?> outputType,
                                  int value, int encoder, int schemaLocal, int seenRefs) {
    Schema.Type schemaType = schema.getType();

    switch (schemaType) {
      case NULL:
        break;
      case BOOLEAN:
        encodeSimple(mg, outputType, schema, "writeBool", value, encoder);
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
      case STRING:
        String encodeMethod = "write" + schemaType.name().charAt(0) + schemaType.name().substring(1).toLowerCase();
        encodeSimple(mg, outputType, schema, encodeMethod, value, encoder);
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

          encodeCollection(mg, componentType, schema.getComponentSchema(),
                           value, encoder, schemaLocal, seenRefs);
        } else if (outputType.isArray()) {
          TypeToken<?> componentType = outputType.getComponentType();
          encodeArray(mg, componentType, schema.getComponentSchema(),
                      value, encoder, schemaLocal, seenRefs);
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
   * @param mg Method body generator
   * @param intValue The integer constant value to encode
   * @param encoder Method argument index of the encoder
   */
  private void encodeInt(GeneratorAdapter mg, int intValue, int encoder) {
    mg.loadArg(encoder);
    mg.push(intValue);
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, "writeInt", int.class));
    mg.pop();
  }

  /**
   * Generates method body for encoding simple schema type by calling corresponding write method in Encoder.
   * @param mg Method body generator
   * @param type Data type to encode
   * @param encodeMethod Name of the encode method to invoke on the given encoder.
   * @param value Argument index of the value to encode.
   * @param encoder Method argument index of the encoder
   */
  private void encodeSimple(GeneratorAdapter mg, TypeToken<?> type, Schema schema,
                            String encodeMethod, int value, int encoder) {
    // encoder.writeXXX(value);
    TypeToken<?> encodeType = type;
    mg.loadArg(encoder);
    mg.loadArg(value);
    if (Primitives.isWrapperType(encodeType.getRawType())) {
      encodeType = TypeToken.of(Primitives.unwrap(encodeType.getRawType()));
      mg.unbox(Type.getType(encodeType.getRawType()));
      // A special case since INT type represents (byte, char, short and int).
      if (schema.getType() == Schema.Type.INT && !int.class.equals(encodeType.getRawType())) {
        encodeType = TypeToken.of(int.class);
      }
    } else if (schema.getType() == Schema.Type.STRING && !String.class.equals(encodeType.getRawType())) {
      // For non-string object that has a String schema, invoke toString().
      mg.invokeVirtual(Type.getType(encodeType.getRawType()), getMethod(String.class, "toString"));
      encodeType = TypeToken.of(String.class);
    } else if (schema.getType() == Schema.Type.BYTES && UUID.class.equals(encodeType.getRawType())) {
      // Special case UUID, encode as byte array

      // ByteBuffer buf = ByteBuffer.allocate(Longs.BYTES * 2)
      //                            .putLong(uuid.getMostSignificantBits())
      //                            .putLong(uuid.getLeastSignificantBits());
      // encoder.writeBytes((ByteBuffer) buf.flip());

      Type byteBufferType = Type.getType(ByteBuffer.class);
      Type uuidType = Type.getType(UUID.class);

      mg.push(Longs.BYTES * 2);
      mg.invokeStatic(byteBufferType, getMethod(ByteBuffer.class, "allocate", int.class));
      mg.swap();

      mg.invokeVirtual(uuidType, getMethod(long.class, "getMostSignificantBits"));
      mg.invokeVirtual(byteBufferType, getMethod(ByteBuffer.class, "putLong", long.class));

      mg.loadArg(value);
      mg.invokeVirtual(uuidType, getMethod(long.class, "getLeastSignificantBits"));
      mg.invokeVirtual(byteBufferType, getMethod(ByteBuffer.class, "putLong", long.class));

      mg.invokeVirtual(Type.getType(Buffer.class), getMethod(Buffer.class, "flip"));
      mg.checkCast(byteBufferType);

      encodeType = TypeToken.of(ByteBuffer.class);
    }
    mg.invokeInterface(Type.getType(Encoder.class), getMethod(Encoder.class, encodeMethod, encodeType.getRawType()));
    mg.pop();
  }

  /**
   * Generates method body for encoding enum value.
   * @param mg Method body generator
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
   * Generates method body for encoding Collection value. The logic is like this:
   *
   * <pre>
   * {@code
   *
   * encoder.writeInt(collection.size());
   * for (T element : collection) {
   *   encodeElement(element, encoder, elementSchema, seenRefs);
   * }
   * if (collection.size() > 0) {
   *   encoder.writeInt(0);
   * }
   * }
   * </pre>
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

    // Call the encode method for encoding the element.
    mg.loadThis();

    mg.loadLocal(iterator);
    mg.invokeInterface(Type.getType(Iterator.class), getMethod(Object.class, "next"));
    doCast(mg, componentType, componentSchema);
    mg.loadArg(encoder);
    mg.loadLocal(componentSchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(classType, getEncodeMethod(componentType, componentSchema));
    mg.goTo(beginFor);

    mg.mark(endFor);

    // if length > 0, write out 0 at the end of array.
    Label zeroLength = mg.newLabel();
    mg.loadLocal(length);
    mg.ifZCmp(GeneratorAdapter.LE, zeroLength);
    encodeInt(mg, 0, encoder);
    mg.mark(zeroLength);
  }

  /**
   * Generates method body for encoding array value. The logic is similar to the one in
   * {@link #encodeCollection}, with collection size replaced with array length.
   *
   * @param mg
   * @param componentType
   * @param componentSchema
   * @param value
   * @param encoder
   * @param schemaLocal
   * @param seenRefs
   */
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

    // Call encode method to encode array[idx]
    mg.loadThis();
    mg.loadArg(value);
    mg.loadLocal(idx);
    TypeToken<?> callTypeToken = getCallTypeToken(componentType, componentSchema);
    mg.arrayLoad(Type.getType(callTypeToken.getRawType()));
    mg.loadArg(encoder);
    mg.loadLocal(componentSchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(classType, getEncodeMethod(componentType, componentSchema));

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

  /**
   * Generates method body for encoding map value. The logic is like this:
   *
   * <pre>
   * {@code
   *
   * encoder.writeInt(map.size();
   *
   * for (Map.Entry<Key, Value> entry : map.entrySet()) {
   *   encodeKey(entry.getKey(), encoder, keySchema, seenRefs);
   *   encodeValue(entry.getValue(), encoder, valueSchema, seenRefs);
   * }
   *
   * if (map.size() > 0) {
   *   encoder.writeInt(0);
   * }
   * }
   * </pre>
   *
   * @param mg
   * @param keyType
   * @param valueType
   * @param keySchema
   * @param valueSchema
   * @param value
   * @param encoder
   * @param schemaLocal
   * @param seenRefs
   */
  private void encodeMap(GeneratorAdapter mg, TypeToken<?> keyType, TypeToken<?> valueType,
                         Schema keySchema, Schema valueSchema, int value, int encoder, int schemaLocal, int seenRefs) {
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
    doCast(mg, keyType, keySchema);
    mg.loadArg(encoder);
    mg.loadLocal(keySchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(classType, getEncodeMethod(keyType, keySchema));

    // encode value
    mg.loadThis();
    mg.loadLocal(entry);
    mg.invokeInterface(Type.getType(Map.Entry.class), getMethod(Object.class, "getValue"));
    doCast(mg, valueType, valueSchema);
    mg.loadArg(encoder);
    mg.loadLocal(valueSchemaLocal);
    mg.loadArg(seenRefs);
    mg.invokeVirtual(classType, getEncodeMethod(valueType, valueSchema));

    mg.goTo(beginFor);
    mg.mark(endFor);

    // if length > 0, write out 0 at the end of map
    Label zeroLength = mg.newLabel();
    mg.loadLocal(length);
    mg.ifZCmp(GeneratorAdapter.LE, zeroLength);
    encodeInt(mg, 0, encoder);
    mg.mark(zeroLength);
  }

  /**
   * Generates method body for encoding java class. If the class given is an interface,
   * getter method will be used to access the field values, otherwise, it will assumes
   * fields are public.
   *
   * @param mg
   * @param schema
   * @param outputType
   * @param value
   * @param encoder
   * @param schemaLocal
   * @param seenRefs
   */
  private void encodeRecord(GeneratorAdapter mg, Schema schema, TypeToken<?> outputType,
                            int value, int encoder, int schemaLocal, int seenRefs) {

    try {
      Class<?> rawType = outputType.getRawType();
      boolean isInterface = rawType.isInterface();

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
        if (isInterface) {
          mg.loadThis();
          mg.loadArg(value);
          Method getter = getGetter(outputType, field.getName());
          fieldType = outputType.resolveType(rawType.getMethod(getter.getName()).getGenericReturnType());
          mg.invokeInterface(Type.getType(rawType), getter);
        } else {
          fieldType = outputType.resolveType(Fields.findField(outputType, field.getName()).getGenericType());
          fieldAccessorRequests.put(outputType, field.getName());
          mg.loadThis();
          mg.dup();
          mg.getField(classType, getFieldAccessorName(outputType, field.getName()), Type.getType(FieldAccessor.class));
          mg.loadArg(value);
          mg.invokeInterface(Type.getType(FieldAccessor.class), getAccessorMethod(fieldType));
          if (!fieldType.getRawType().isPrimitive()) {
            doCast(mg, fieldType, field.getSchema());
          }
        }
        mg.loadArg(encoder);
        mg.loadLocal(fieldSchemas);
        mg.push(i);
        mg.invokeInterface(Type.getType(List.class), getMethod(Object.class, "get", int.class));
        mg.checkCast(Type.getType(Schema.Field.class));
        mg.invokeVirtual(Type.getType(Schema.Field.class), getMethod(Schema.class, "getSchema"));
        mg.loadArg(seenRefs);
        mg.invokeVirtual(classType, getEncodeMethod(fieldType, field.getSchema()));
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Generates method body for encoding union schema. Union schema is used for representing object references that
   * could be {@code null}.
   * @param mg
   * @param outputType
   * @param schema
   * @param value
   * @param encoder
   * @param schemaLocal
   * @param seenRefs
   */
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
    doCast(mg, outputType, schema.getUnionSchema(0));
    mg.loadArg(encoder);
    mg.loadArg(schemaLocal);
    mg.push(0);
    mg.invokeVirtual(Type.getType(Schema.class), getMethod(Schema.class, "getUnionSchema", int.class));
    mg.loadArg(seenRefs);
    mg.invokeVirtual(classType, getEncodeMethod(outputType, schema.getUnionSchema(0)));

    mg.goTo(endLabel);

    mg.mark(nullLabel);
    // Null, write out 1
    encodeInt(mg, 1, encoder);
    mg.mark(endLabel);
  }

  private <T> TypeToken<DatumWriter<T>> getInterfaceType(TypeToken<T> type) {
    return new TypeToken<DatumWriter<T>>() {
    }.where(new TypeParameter<T>() {
    }, type);
  }

  private <T> TypeToken<T[]> getArrayType(TypeToken<T> type) {
    return new TypeToken<T[]>() {
    }.where(new TypeParameter<T>() {
    }, type);
  }

  private String getClassName(TypeToken<?> interfaceType, Schema schema) {
    return String.format("%s/%s%s%s",
                         interfaceType.getRawType().getPackage().getName().replace('.', '/'),
                         normalizeTypeName(TypeToken.of(((ParameterizedType) interfaceType.getType())
                                                          .getActualTypeArguments()[0])),
                         interfaceType.getRawType().getSimpleName(), schema.getSchemaHash());
  }

  private String normalizeTypeName(TypeToken<?> type) {
    String typeName = type.toString();
    int dimension = 0;
    while (type.isArray()) {
      type = type.getComponentType();
      typeName = type.toString();
      dimension++;
    }

    typeName = typeName.replace(".", "")
                        .replace("<", "Of")
                        .replace(">", "")
                        .replace(",", "To")
                        .replace(" ", "")
                        .replace("$", "");
    if (dimension > 0) {
      typeName = "Array" + dimension + typeName;
    }
    return typeName;
  }

  private Method getMethod(Class<?> returnType, String name, Class<?>...args) {
    return Methods.getMethod(returnType, name, args);
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

  /**
   * Returns the type to be used on the encode method. This is needed to work with private classes
   * that the generated DatumWriter doesn't have access to.
   *
   * @param outputType Type information of the data type for output
   * @param schema Schema to use for output.
   * @return The type information to be used for encode method.
   */
  private TypeToken<?> getCallTypeToken(TypeToken<?> outputType, Schema schema) {
    Schema.Type schemaType = schema.getType();

    if (schemaType == Schema.Type.RECORD || schemaType == Schema.Type.UNION) {
      return TypeToken.of(Object.class);
    }
    if (schemaType == Schema.Type.ARRAY && outputType.isArray()) {
      return getArrayType(getCallTypeToken(outputType.getComponentType(), schema.getComponentSchema()));
    }

    return outputType;
  }

  /**
   * Optionally generates a type cast instruction based on the result of
   * {@link #getCallTypeToken(com.google.common.reflect.TypeToken, Schema)}.
   * @param mg A {@link GeneratorAdapter} for generating instructions
   * @param outputType
   * @param schema
   */
  private void doCast(GeneratorAdapter mg, TypeToken<?> outputType, Schema schema) {
    TypeToken<?> callTypeToken = getCallTypeToken(outputType, schema);
    if (!Object.class.equals(callTypeToken.getRawType()) && !outputType.getRawType().isPrimitive()) {
      mg.checkCast(Type.getType(callTypeToken.getRawType()));
    }
  }

  /**
   * Returns the method for calling {@link FieldAccessor} based on the data type.
   * @param type Data type.
   * @return A {@link Method} for calling {@link FieldAccessor}.
   */
  private Method getAccessorMethod(TypeToken<?> type) {
    Class<?> rawType = type.getRawType();
    if (rawType.isPrimitive()) {
      return getMethod(rawType,
                       String.format("get%c%s",
                                     Character.toUpperCase(rawType.getName().charAt(0)),
                                     rawType.getName().substring(1)),
                       Object.class);
    } else {
      return getMethod(Object.class, "get", Object.class);
    }
  }

  /**
   * Generates the name of the class field for storing {@link FieldAccessor} for the given record field.
   * @param recordType Type of the record.
   * @param fieldName name of the field.
   * @return name of the class field.
   */
  private String getFieldAccessorName(TypeToken<?> recordType, String fieldName) {
    return String.format("%s$%s", normalizeTypeName(recordType), fieldName);
  }
}
