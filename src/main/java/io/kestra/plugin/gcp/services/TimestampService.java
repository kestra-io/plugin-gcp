package io.kestra.plugin.gcp.services;

import com.google.protobuf.Timestamp;

import java.time.Instant;

public abstract class TimestampService {
    public static Instant of(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
