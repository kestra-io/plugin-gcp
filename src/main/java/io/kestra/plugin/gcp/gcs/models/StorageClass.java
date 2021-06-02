package io.kestra.plugin.gcp.gcs.models;

public enum StorageClass {
    REGIONAL,
    MULTI_REGIONAL,
    NEARLINE,
    COLDLINE,
    STANDARD,
    ARCHIVE,
    DURABLE_REDUCED_AVAILABILITY
}
