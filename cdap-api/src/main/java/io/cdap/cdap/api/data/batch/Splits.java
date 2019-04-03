/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.data.batch;

import co.cask.cdap.api.common.Bytes;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * Utility class to serialize and deserialize {@link Split}.
 */
public final class Splits {

  /**
   * Serialize a list of {@link Splits}s to a {@link String}.
   *
   * @param splits the list of splits to serialize
   * @return a {@link String} representation of the splits.
   * @throws IOException if failed to serialize
   */
  public static String encode(Iterable<? extends Split> splits) throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(os)) {
      for (Split split : splits) {
        serialize(split, out);
      }
    }
    return Bytes.toStringBinary(os.toByteArray());
  }

  /**
   * Deserialize a list of {@link Split}s encoded by the {@link #encode(Iterable)} method
   *
   * @param encoded the encoded string produced by the {@link #encode(Iterable)} method
   * @param splits a {@link Collection} for storing the decoded splits
   * @param classLoader the {@link ClassLoader} for loading the {@link Split} class
   * @param <T> type of the {@link Collection}
   * @return the same {@link Collection} passed in the arguments
   * @throws IOException if failed to deserialize
   * @throws ClassNotFoundException if failed to load the {@link Split} class.
   */
  public static <T extends Collection<? super Split>> T decode(String encoded, T splits, ClassLoader classLoader)
    throws IOException, ClassNotFoundException {

    InputStream is = new ByteArrayInputStream(Bytes.toBytesBinary(encoded));
    try (DataInputStream in = new DataInputStream(is)) {
      while (in.available() > 0) {
        splits.add(deserialize(in, classLoader));
      }
    }
    return splits;
  }

  /**
   * Serialize a {@link Split} instance.
   *
   * @param split the {@link Split} to serialize
   * @param out the {@link DataOutput} that the serialized data is writing to
   * @throws IOException if failed to serialize
   */
  public static void serialize(Split split, DataOutput out) throws IOException {
    try {
      // Write out the class name
      out.writeUTF(split.getClass().getName());
      // Write out the split. For custom split that doesn't override the writeExternal method,
      // an UnsupportedOperationException will be thrown.
      // Also if the Split doesn't have a public default constructor, use Gson as well
      if ((split.getClass().getConstructor().getModifiers() & Modifier.PUBLIC) == Modifier.PUBLIC) {
        split.writeExternal(out);
        return;
      }
    } catch (UnsupportedOperationException | NoSuchMethodException e) {
      // Use gson
    }

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    try (Writer writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
      new Gson().toJson(split, writer);
    }
    byte[] bytes = os.toByteArray();
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  /**
   * Deserialize a {@link Split} that was written by the {@link #serialize(Split, DataOutput)} method
   * using a provided {@link ClassLoader} to load the {@link Split} class.
   *
   * @param in the {@link DataInput} for reading the serialized data
   * @param classLoader the {@link ClassLoader} for loading the actual {@link Split} class
   * @return a {@link Split} instance
   * @throws IOException if failed to deserialize
   * @throws ClassNotFoundException if failed to load the split class as indicated by the encoded string
   */
  public static Split deserialize(DataInput in, ClassLoader classLoader) throws IOException, ClassNotFoundException {
    String className = in.readUTF();
    try {
      Split split = (Split) classLoader.loadClass(className).getConstructor().newInstance();
      split.readExternal(in);
      return split;
    } catch (InstantiationException | IllegalAccessException | NoSuchMethodException |
             InvocationTargetException | UnsupportedOperationException e) {
      // If failed to instantiate the class using default constructor or the readExternal
      // throws UnsupportedOperationException, deserialize with Gson
    }

    // Use Gson if failed to do readExternal.
    byte[] bytes = new byte[in.readInt()];
    in.readFully(bytes);
    return (Split) new Gson().fromJson(new InputStreamReader(new ByteArrayInputStream(bytes),
                                                             StandardCharsets.UTF_8), classLoader.loadClass(className));
  }

  private Splits() {
    // no-op
  }
}
