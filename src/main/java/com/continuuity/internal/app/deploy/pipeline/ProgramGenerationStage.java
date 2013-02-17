package com.continuuity.internal.app.deploy.pipeline;

import com.continuuity.archive.ArchiveBundler;
import com.continuuity.filesystem.Location;
import com.continuuity.pipeline.AbstractStage;
import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;

import javax.annotation.Nullable;
import java.io.File;
import java.util.zip.ZipEntry;

/**
 *
 */
public class ProgramGenerationStage extends AbstractStage<ApplicationSpecLocation> {
  private final Predicate<ZipEntry> metaIgnore = new Predicate<ZipEntry>() {
    @Override
    public boolean apply(@Nullable ZipEntry input) {
      return input.getName().contains("MANIFEST.MF");
    }
  };

  public ProgramGenerationStage() {
    super(TypeToken.of(ApplicationSpecLocation.class));
  }

  @Override
  public void process(final ApplicationSpecLocation o) throws Exception {
    ArchiveBundler bundler = new ArchiveBundler(o.getArchive());
  }
}
