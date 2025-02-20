/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.cdap.spi.data.compression;

import java.io.IOException;

/**
 * An interface for compressing and decompressing data.
 */
public interface Compressor {

  /**
   * Compresses the given string data.
   *
   * @param data the string data to compress
   * @return the compressed string data
   * @throws IOException if there is an error during compression
   */
  String compress(String data) throws IOException;

  /**
   * Compresses the given byte array data.
   *
   * @param data the byte array data to compress
   * @return the compressed byte array data
   * @throws IOException if there is an error during compression
   */
  byte[] compress(byte[] data) throws IOException;

  /**
   * Decompresses the given compressed string data.
   *
   * @param compressedData the compressed string data
   * @return the decompressed string data
   * @throws IOException if there is an error during decompression
   */
  String decompress(String compressedData) throws IOException;

  /**
   * Decompresses the given compressed byte array data.
   *
   * @param compressedData the compressed byte array data
   * @return the decompressed byte array data
   * @throws IOException if there is an error during decompression
   */
  byte[] decompress(byte[] compressedData) throws IOException;
}
