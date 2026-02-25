package io.kestra.plugin.gcp.services;

import java.time.Instant;

import com.google.protobuf.Timestamp;

public abstract class TimestampService {
    public static Instant of(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}
