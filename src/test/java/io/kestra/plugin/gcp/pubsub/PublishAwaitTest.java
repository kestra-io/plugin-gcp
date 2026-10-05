package io.kestra.plugin.gcp.pubsub;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PublishAwaitTest {
    @Test
    void shouldWaitForAllMessagesAndClearThem() {
        List<ApiFuture<String>> futures = new ArrayList<>(
            List.of(ApiFutures.immediateFuture("message-1"), ApiFutures.immediateFuture("message-2"))
        );

        assertDoesNotThrow(() -> Publish.awaitPublished(futures));

        assertThat(futures, empty());
    }

    @Test
    void shouldReturnWhenThereIsNothingToPublish() {
        assertDoesNotThrow(() -> Publish.awaitPublished(new ArrayList<>()));
    }

    @Test
    void shouldFailWhenAMessageCouldNotBePublished() {
        List<ApiFuture<String>> futures = new ArrayList<>(
            List.of(
                ApiFutures.immediateFuture("message-1"),
                ApiFutures.immediateFailedFuture(new IllegalStateException("NOT_FOUND: Resource not found (resource=missing-topic)"))
            )
        );

        var exception = assertThrows(IllegalStateException.class, () -> Publish.awaitPublished(futures));

        assertThat(exception.getMessage(), containsString("missing-topic"));
    }
}
