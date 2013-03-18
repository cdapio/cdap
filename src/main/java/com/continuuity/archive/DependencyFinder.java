package com.continuuity.archive;

import com.google.common.base.Function;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to help find all class dependencies of a given class
 */
public class DependencyFinder {

  private static final int FLAGS = ClassReader.SKIP_CODE + ClassReader.SKIP_DEBUG + ClassReader.SKIP_FRAMES;

  public interface ClassFinder {
    /**
     * Returns the bytecode of the given class, or {@code null} if it is not found.
     * @param className Name of the class.
     * @return
     */
    byte[] getByteCode(String className);
  }

  public interface ClassDependencies {

    /**
     * Returns immutable set of class name that are dependencies from the given class.
     * @param className
     * @return
     */
    Set<String> get(String className);

    /**
     * Returns an immutable set containing all class names that are found during the dependency search.
     * @return
     */
    Set<String> getAll();
  }

  /**
   * Returns a {@link ClassDependencies} instance containing dependency information of those could
   * be found with the given {@link ClassFinder}.
   *
   * @param clz
   * @param classFinder
   * @return
   */
  public ClassDependencies find(String clz, final ClassFinder classFinder) throws IOException {
    SetMultimap<String, String> dependencies = HashMultimap.create();

    ClassReader cr = new ClassReader(classFinder.getByteCode(clz));
    cr.accept(new DependencyClassVisitor(dependencies, classFinder, Sets.<String>newHashSet()), FLAGS);

    return new DefaultClassDependencies(dependencies);
  }

  private static final class DefaultClassDependencies implements ClassDependencies {

    private static final Function<String, String> NAME_TRANSFORMER = new Function<String, String>() {
      @Override
      public String apply(String input) {
        return Type.getObjectType(input).getClassName();
      }
    };
    private final SetMultimap<String, String> dependencies;

    private DefaultClassDependencies(SetMultimap<String, String> dependencies) {
      this.dependencies = dependencies;
    }

    @Override
    public Set<String> get(String className) {
      return ImmutableSet.copyOf(
        Iterables.transform(dependencies.get(className.replace('.', '/')), NAME_TRANSFORMER));
    }

    @Override
    public Set<String> getAll() {
      return ImmutableSet.copyOf(
        Iterables.transform(Iterables.concat(dependencies.keySet(), dependencies.values()),
                            NAME_TRANSFORMER));
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      for (Map.Entry<String, String> entry : dependencies.entries()) {
        builder.append(Type.getObjectType(entry.getKey()).getClassName())
          .append(" => ")
          .append(Type.getObjectType(entry.getValue()).getClassName())
          .append(System.getProperty("line.separator"));
      }
      return builder.length() > 0
        ? builder.substring(0, builder.length() - System.getProperty("line.separator").length())
        : "";
    }
  }

  private static final class DependencyClassVisitor extends ClassVisitor {

    private final Multimap<String, String> dependencies;
    private final ClassFinder classFinder;
    private final SignatureVisitor signatureVisitor;
    private final Set<String> seenClasses;
    private String className;

    public DependencyClassVisitor(Multimap<String, String> dependencies,
                                  ClassFinder classFinder, Set<String> seenClasses) {
      super(Opcodes.ASM4);
      this.dependencies = dependencies;
      this.classFinder = classFinder;
      this.seenClasses = seenClasses;
      this.signatureVisitor = new SignatureVisitor(Opcodes.ASM4) {
        private String currentClass;

        @Override
        public void visitClassType(String name) {
          currentClass = name;
          addClass(name);
        }

        @Override
        public void visitInnerClassType(String name) {
          addClass(currentClass + "$" + name);
        }
      };
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      className = name;

      if (signature != null) {
        new SignatureReader(signature).accept(signatureVisitor);
      } else {
        if (superName != null) {
          addClass(superName);
        }
        if (interfaces != null) {
          for (String intf : interfaces) {
            addClass(intf);
          }
        }
      }
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc) {
      addClass(owner);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
      addType(Type.getType(desc));
      return null;
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
      addClass(name);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
      if (signature != null) {
        new SignatureReader(signature).acceptType(signatureVisitor);
      } else {
        addType(Type.getType(desc));
      }

      return new FieldVisitor(Opcodes.ASM4) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          addType(Type.getType(desc));
          return null;
        }
      };
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
      if (signature != null) {
        new SignatureReader(signature).accept(signatureVisitor);
      } else {
        addMethod(desc);
      }

      return  new MethodVisitor(Opcodes.ASM4) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          addType(Type.getType(desc));
          return null;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
          addType(Type.getType(desc));
          return null;
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
          addType(Type.getObjectType(type));
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
          addType(Type.getObjectType(owner));
          addType(Type.getType(desc));
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc) {
          addType(Type.getObjectType(owner));
          addMethod(desc);
        }

        @Override
        public void visitLdcInsn(Object cst) {
          if (cst instanceof Type) {
            addType((Type)cst);
          }
        }

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims) {
          addType(Type.getType(desc));
        }

        @Override
        public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
          if (signature != null) {
            new SignatureReader(signature).acceptType(signatureVisitor);
          } else {
            addType(Type.getType(desc));
          }
        }
      };
    }

    private void addClass(String internalName) {
      if (className.equals(internalName)) {
        return;
      }
      byte[] bytecode = classFinder.getByteCode(Type.getObjectType(internalName).getClassName());
      if (bytecode != null) {
        if (seenClasses.add(internalName)) {
          new ClassReader(bytecode).accept(new DependencyClassVisitor(dependencies, classFinder, seenClasses), FLAGS);
        }

        dependencies.put(className, internalName);
      }
    }

    private void addType(Type type) {
      if (type.getSort() == Type.ARRAY) {
        type = type.getElementType();
      }
      if (type.getSort() == Type.OBJECT) {
        addClass(type.getInternalName());
      }
    }

    private void addMethod(String desc) {
      addType(Type.getReturnType(desc));
      for (Type type : Type.getArgumentTypes(desc)) {
        addType(type);
      }
    }
  }
}
