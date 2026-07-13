package io.kestra.plugin.gcp.rcs;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest
@WireMockTest
public class RcsTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private GoogleCredentials mockCredentials() throws Exception {
        var mockCredentials = Mockito.mock(GoogleCredentials.class);
        var scopedCredentials = Mockito.mock(GoogleCredentials.class);
        var mockToken = new AccessToken("mock-oauth-token", new Date(System.currentTimeMillis() + 3600000));

        Mockito.doReturn(scopedCredentials).when(mockCredentials).createScoped(Mockito.any(List.class));
        Mockito.doReturn(mockToken).when(scopedCredentials).refreshAccessToken();
        Mockito.doReturn(mockToken).when(mockCredentials).refreshAccessToken();
        return mockCredentials;
    }

    private void stubRbmEndpoints() {
        stubFor(
            post(urlPathMatching("/v1/phones/[^/]+/agentMessages"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "phones/+33612345678/agentMessages/msg-123",
                              "messageId": "msg-123",
                              "sendTime": "2026-07-10T14:33:53Z"
                            }
                            """)
                )
        );

        stubFor(
            post(urlPathMatching("/v1/files"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "files/file-123",
                              "mimeType": "application/pdf"
                            }
                            """)
                )
        );

        stubFor(
            post(urlPathMatching("/upload/v1/files"))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("""
                            {
                              "name": "files/file-binary-123",
                              "mimeType": "application/octet-stream"
                            }
                            """)
                )
        );
    }

    @Test
    void sendMessage(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubRbmEndpoints();

        var task = SendMessage.builder()
            .id(RcsTest.class.getSimpleName())
            .type(SendMessage.class.getName())
            .baseUrl(wmRuntimeInfo.getHttpBaseUrl())
            .agentId(Property.ofValue("agent-123"))
            .msisdn(Property.ofValue("+33612345678"))
            .text(Property.ofValue("Your order has been shipped."))
            .build();

        var spyTask = Mockito.spy(task);
        Mockito.doReturn(mockCredentials()).when(spyTask).credentials(Mockito.any(RunContext.class));

        var runContext = TestsUtils.mockRunContext(runContextFactory, spyTask, ImmutableMap.of());
        var output = spyTask.run(runContext);

        assertThat(output.getMessageId(), is("msg-123"));
        assertThat(output.getSendTime(), is(Instant.parse("2026-07-10T14:33:53Z")));

        verify(
            postRequestedFor(urlPathEqualTo("/v1/phones/%2B33612345678/agentMessages"))
                .withQueryParam("agentId", equalTo("agent-123"))
                .withHeader("Content-Type", equalTo("application/json"))
                .withRequestBody(containing("Your order has been shipped."))
        );
    }

    @Test
    void sendRichCard(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubRbmEndpoints();

        var task = SendRichCard.builder()
            .id(RcsTest.class.getSimpleName())
            .type(SendRichCard.class.getName())
            .baseUrl(wmRuntimeInfo.getHttpBaseUrl())
            .agentId(Property.ofValue("agent-123"))
            .msisdn(Property.ofValue("+33612345678"))
            .title(Property.ofValue("Delivery Alert"))
            .contentDescription(Property.ofValue("Your package is ready."))
            .imageUrl(Property.ofValue("https://example.com/banner.jpg"))
            .suggestions(
                List.of(
                    Suggestion.builder()
                        .type(Property.ofValue(SuggestionType.URL))
                        .text(Property.ofValue("Track"))
                        .url(Property.ofValue("https://tracking.com"))
                        .build(),
                    Suggestion.builder()
                        .type(Property.ofValue(SuggestionType.REPLY))
                        .text(Property.ofValue("Confirm"))
                        .postbackData(Property.ofValue("confirm_data"))
                        .build()
                )
            )
            .build();

        var spyTask = Mockito.spy(task);
        Mockito.doReturn(mockCredentials()).when(spyTask).credentials(Mockito.any(RunContext.class));

        var runContext = TestsUtils.mockRunContext(runContextFactory, spyTask, ImmutableMap.of());
        var output = spyTask.run(runContext);

        assertThat(output.getMessageId(), is("msg-123"));

        verify(
            postRequestedFor(urlPathEqualTo("/v1/phones/%2B33612345678/agentMessages"))
                .withRequestBody(containing("Delivery Alert"))
                .withRequestBody(containing("Track"))
                .withRequestBody(containing("confirm_data"))
        );
    }

    @Test
    void sendCarousel(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubRbmEndpoints();

        var task = SendCarousel.builder()
            .id(RcsTest.class.getSimpleName())
            .type(SendCarousel.class.getName())
            .baseUrl(wmRuntimeInfo.getHttpBaseUrl())
            .agentId(Property.ofValue("agent-123"))
            .msisdn(Property.ofValue("+33612345678"))
            .cards(
                List.of(
                    Card.builder()
                        .title(Property.ofValue("Card 1"))
                        .description(Property.ofValue("Description 1"))
                        .imageUrl(Property.ofValue("https://example.com/c1.jpg"))
                        .build(),
                    Card.builder()
                        .title(Property.ofValue("Card 2"))
                        .description(Property.ofValue("Description 2"))
                        .imageUrl(Property.ofValue("https://example.com/c2.jpg"))
                        .build()
                )
            )
            .build();

        var spyTask = Mockito.spy(task);
        Mockito.doReturn(mockCredentials()).when(spyTask).credentials(Mockito.any(RunContext.class));

        var runContext = TestsUtils.mockRunContext(runContextFactory, spyTask, ImmutableMap.of());
        var output = spyTask.run(runContext);

        assertThat(output.getMessageId(), is("msg-123"));

        verify(
            postRequestedFor(urlPathEqualTo("/v1/phones/%2B33612345678/agentMessages"))
                .withRequestBody(containing("Card 1"))
                .withRequestBody(containing("Card 2"))
        );
    }

    @Test
    void sendFileFromUrl(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubRbmEndpoints();

        var task = SendFile.builder()
            .id(RcsTest.class.getSimpleName())
            .type(SendFile.class.getName())
            .baseUrl(wmRuntimeInfo.getHttpBaseUrl())
            .agentId(Property.ofValue("agent-123"))
            .msisdn(Property.ofValue("+33612345678"))
            .file(Property.ofValue("https://example.com/file.pdf"))
            .build();

        var spyTask = Mockito.spy(task);
        Mockito.doReturn(mockCredentials()).when(spyTask).credentials(Mockito.any(RunContext.class));

        var runContext = TestsUtils.mockRunContext(runContextFactory, spyTask, ImmutableMap.of());
        var output = spyTask.run(runContext);

        assertThat(output.getMessageId(), is("msg-123"));

        verify(
            postRequestedFor(urlPathEqualTo("/v1/files"))
                .withRequestBody(containing("https://example.com/file.pdf"))
        );

        verify(
            postRequestedFor(urlPathEqualTo("/v1/phones/%2B33612345678/agentMessages"))
                .withRequestBody(containing("files/file-123"))
        );
    }

    @Test
    void sendFileFromStorage(WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        stubRbmEndpoints();

        var storageUri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/test-attachment.pdf"),
            new ByteArrayInputStream("dummy pdf content".getBytes())
        );

        var task = SendFile.builder()
            .id(RcsTest.class.getSimpleName())
            .type(SendFile.class.getName())
            .baseUrl(wmRuntimeInfo.getHttpBaseUrl())
            .agentId(Property.ofValue("agent-123"))
            .msisdn(Property.ofValue("+33612345678"))
            .file(Property.ofValue(storageUri.toString()))
            .build();

        var spyTask = Mockito.spy(task);
        Mockito.doReturn(mockCredentials()).when(spyTask).credentials(Mockito.any(RunContext.class));

        var runContext = TestsUtils.mockRunContext(runContextFactory, spyTask, ImmutableMap.of());
        var output = spyTask.run(runContext);

        assertThat(output.getMessageId(), is("msg-123"));

        verify(
            postRequestedFor(urlPathEqualTo("/upload/v1/files"))
                .withHeader("Content-Type", equalTo("application/pdf"))
                .withRequestBody(equalTo("dummy pdf content"))
        );

        verify(
            postRequestedFor(urlPathEqualTo("/v1/phones/%2B33612345678/agentMessages"))
                .withRequestBody(containing("files/file-binary-123"))
        );
    }
}
