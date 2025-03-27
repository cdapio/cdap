package io.cdap.cdap.metadata.spanner;

import io.cdap.cdap.spi.metadata.Metadata;

import javax.annotation.Nullable;

/**
 * A metadata and it version in the index. Used for optimistic concurrency control.
 */
public class VersionedMetadata {

    private final Metadata metadata;
    private final Long version;

    static final VersionedMetadata NONE = new VersionedMetadata(Metadata.EMPTY, null);

    static VersionedMetadata of(Metadata metadata, long version) {
        return new VersionedMetadata(metadata, version);
    }

    private VersionedMetadata(Metadata metadata, @Nullable Long version) {
        this.metadata = metadata;
        this.version = version;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public Long getVersion() {
        return version;
    }

    public boolean existing() {
        return version != null;
    }

}
