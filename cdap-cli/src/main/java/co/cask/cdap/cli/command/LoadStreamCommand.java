/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.FilePathResolver;
import co.cask.cdap.client.StreamClient;
import co.cask.cdap.proto.id.StreamId;
import co.cask.common.cli.Arguments;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.inject.Inject;

import java.io.File;
import java.io.PrintStream;
import java.util.Map;

/**
 * Command for sending file to stream
 */
public class LoadStreamCommand extends AbstractAuthCommand implements Categorized {

  // A map from file extension to content type
  private static final Map<String, String> CONTENT_TYPE_MAP;

  static {
    Map<String, String> contentTypes = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    contentTypes.put("avro", "avro/binary");
    contentTypes.put("csv", "text/csv");
    contentTypes.put("tsv", "text/tsv");
    contentTypes.put("txt", "text/plain");
    contentTypes.put("log", "text/plain");
    CONTENT_TYPE_MAP = contentTypes;
  }

  private final StreamClient streamClient;
  private final FilePathResolver resolver;

  @Inject
  public LoadStreamCommand(StreamClient streamClient, CLIConfig cliConfig, FilePathResolver resolver) {
    super(cliConfig);
    this.streamClient = streamClient;
    this.resolver = resolver;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    StreamId streamId = cliConfig.getCurrentNamespace().stream(arguments.get(ArgumentName.STREAM.toString()));
    File file = resolver.resolvePathToFile(arguments.get(ArgumentName.LOCAL_FILE_PATH.toString()));
    String contentType = arguments.getOptional(ArgumentName.CONTENT_TYPE.toString(), "");

    if (!file.isFile()) {
      throw new IllegalArgumentException("Not a file: " + file);
    }
    if (contentType.isEmpty()) {
      contentType = getContentType(Files.getFileExtension(file.getName()));
    }
    if (contentType.isEmpty()) {
      throw new IllegalArgumentException("Unsupported file format.");
    }

    streamClient.sendFile(streamId.toId(), contentType, file);
    output.printf("Successfully loaded file to stream '%s'\n", streamId.getEntityName());
  }

  @Override
  public String getPattern() {
    return String.format("load stream <%s> <%s> [<%s>]",
                         ArgumentName.STREAM, ArgumentName.LOCAL_FILE_PATH, ArgumentName.CONTENT_TYPE);
  }

  @Override
  public String getDescription() {
    return String.format("Loads a file to %s. The contents of the file will " +
                         "become multiple events in the %s, " +
                         "based on the content type (%s). If '<%s>' is not provided, " +
                         "it will be detected by the file extension. Supported file extensions: '%s'.",
                         Fragment.of(Article.A, ElementType.STREAM.getName()),
                         ElementType.STREAM.getName(),
                         Joiner.on(", ").join(ImmutableSet.copyOf(CONTENT_TYPE_MAP.values())),
                         ArgumentName.CONTENT_TYPE,
                         Joiner.on("', '").join(CONTENT_TYPE_MAP.keySet()));
  }

  private String getContentType(String extension) {
    String contentType = CONTENT_TYPE_MAP.get(extension);
    return contentType == null ? "" : contentType;
  }

  @Override
  public String getCategory() {
    return CommandCategory.INGEST.getName();
  }
}
