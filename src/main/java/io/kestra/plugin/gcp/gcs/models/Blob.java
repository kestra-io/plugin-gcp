package io.kestra.plugin.gcp.gcs.models;

import lombok.Builder;
import lombok.Data;
import lombok.With;

import java.net.URI;
import java.time.Instant;
import java.util.Map;

@Data
@Builder
public class Blob {
    @With
    private final URI uri;
    private final String bucket;
    private final String name;
    private final String generatedId;
    private final String selfLink;
    private final String cacheControl;
//    private final List<Acl> acl;
//    private final Acl.Entity owner;
    private final Long size;
    private final String etag;
    private final String md5;
    private final String crc32c;
    private final Instant customTime;
    private final String mediaLink;
    private final Map<String, String> metadata;
    private final Long metaGeneration;
    private final Instant deleteTime;
    private final Instant updateTime;
    private final Instant createTime;
    private final String contentType;
    private final String contentEncoding;
    private final String contentDisposition;
    private final String contentLanguage;
//    private final StorageClass storageClass;
    private final Instant timeStorageClassUpdated;
    private final Integer componentCount;
    private final boolean isDirectory;
//    private final BlobInfo.CustomerEncryption customerEncryption;
    private final String kmsKeyName;
    private final Boolean eventBasedHold;
    private final Boolean temporaryHold;
    private final Long retentionExpirationTime;

    public static Blob of(com.google.cloud.storage.Blob blob) {
        return Blob.builder()
            .uri(URI.create("gs://" + blob.getBucket() + "/" + blob.getName()))
            .bucket(blob.getBucket())
            .name(blob.getName())
            .generatedId(blob.getGeneratedId())
            .selfLink(blob.getSelfLink())
            .cacheControl(blob.getCacheControl())
//            .acl(blob.getAcl())
//            .owner(blob.getOwner())
            .size(blob.getSize())
            .etag(blob.getEtag())
            .md5(blob.getMd5())
            .crc32c(blob.getCrc32c())
            .customTime(blob.getCustomTime() == null ? null : Instant.ofEpochMilli(blob.getCustomTime()))
            .mediaLink(blob.getMediaLink())
            .metadata(blob.getMetadata())
            .metaGeneration(blob.getMetageneration())
            .deleteTime(blob.getDeleteTime() == null ? null : Instant.ofEpochMilli(blob.getDeleteTime()))
            .updateTime(blob.getUpdateTime() == null ? null : Instant.ofEpochMilli(blob.getUpdateTime()))
            .createTime(blob.getCreateTime() == null ? null : Instant.ofEpochMilli(blob.getCreateTime()))
            .contentType(blob.getContentType())
            .contentEncoding(blob.getContentEncoding())
            .contentDisposition(blob.getContentDisposition())
            .contentLanguage(blob.getContentLanguage())
//            .storageClass(blob.getStorageClass())
            .timeStorageClassUpdated(blob.getTimeStorageClassUpdated() == null ? null : Instant.ofEpochMilli(blob.getTimeStorageClassUpdated()))
            .componentCount(blob.getComponentCount())
            .isDirectory(blob.isDirectory())
//            .customerEncryption(blob.getCustomerEncryption())
            .kmsKeyName(blob.getKmsKeyName())
            .eventBasedHold(blob.getEventBasedHold())
            .temporaryHold(blob.getTemporaryHold())
            .retentionExpirationTime(blob.getRetentionExpirationTime())
            .build();
    }
}
