package io.kestra.plugin.gcp.vertexai;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.cloud.vertexai.api.Candidate;
import com.google.cloud.vertexai.api.Citation;
import com.google.cloud.vertexai.api.CitationMetadata;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.HarmCategory;
import com.google.cloud.vertexai.api.Part;
import com.google.cloud.vertexai.api.SafetyRating;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class AbstractGenerativeAiTest {

    @Test
    void shouldExtractContentWhenSinglePart() {
        var part = Part.newBuilder().setText("Hello from Gemini!").build();
        var content = Content.newBuilder().addParts(part).build();
        var safetyRating = SafetyRating.newBuilder()
            .setCategory(HarmCategory.HARM_CATEGORY_HATE_SPEECH)
            .setSeverityScore(0.1f)
            .setBlocked(false)
            .build();
        var candidate = Candidate.newBuilder()
            .setContent(content)
            .setFinishReason(Candidate.FinishReason.STOP)
            .addSafetyRatings(safetyRating)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), is("Hello from Gemini!"));
        assertThat(prediction.safetyAttributes().blocked(), is(false));
        assertThat(prediction.safetyAttributes().categories(), hasItem("HARM_CATEGORY_HATE_SPEECH"));
        assertThat(prediction.safetyAttributes().scores(), hasItem(0.1f));
    }

    @Test
    void shouldExtractConcatenatedContentWhenMultipleParts() {
        var part1 = Part.newBuilder().setText("Part 1. ").build();
        var part2 = Part.newBuilder().setText("Part 2.").build();
        var content = Content.newBuilder().addParts(part1).addParts(part2).build();
        var candidate = Candidate.newBuilder()
            .setContent(content)
            .setFinishReason(Candidate.FinishReason.STOP)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), is("Part 1. Part 2."));
        assertThat(prediction.safetyAttributes().blocked(), is(false));
    }

    @Test
    void shouldHandleBlockedCandidateWhenSafetyFinishReasonAndNoParts() {
        var safetyRating = SafetyRating.newBuilder()
            .setCategory(HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT)
            .setSeverityScore(0.92f)
            .setBlocked(false)
            .build();
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.SAFETY)
            .addSafetyRatings(safetyRating)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
        assertThat(prediction.safetyAttributes().categories(), hasItem("HARM_CATEGORY_DANGEROUS_CONTENT"));
        assertThat(prediction.safetyAttributes().scores(), hasItem(0.92f));
    }

    @Test
    void shouldHandleBlockedCandidateWhenRecitationFinishReason() {
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.RECITATION)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
    }

    @Test
    void shouldHandleBlockedCandidateWhenBlocklistFinishReason() {
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.BLOCKLIST)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
    }

    @Test
    void shouldHandleBlockedCandidateWhenProhibitedContentFinishReason() {
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.PROHIBITED_CONTENT)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
    }

    @Test
    void shouldHandleBlockedCandidateWhenSpiiFinishReason() {
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.SPII)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
    }

    @Test
    void shouldHandleBlockedCandidateWhenModelArmorFinishReason() {
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.MODEL_ARMOR)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
    }

    @Test
    void shouldHandleBlockedCandidateWhenSafetyRatingIsExplicitlyBlocked() {
        var safetyRating = SafetyRating.newBuilder()
            .setCategory(HarmCategory.HARM_CATEGORY_HARASSMENT)
            .setSeverityScore(0.88f)
            .setBlocked(true)
            .build();
        var candidate = Candidate.newBuilder()
            .setFinishReason(Candidate.FinishReason.STOP)
            .addSafetyRatings(safetyRating)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(true));
    }

    @Test
    void shouldHandleCandidateWithContentHavingZeroParts() {
        var content = Content.newBuilder().build();
        var candidate = Candidate.newBuilder()
            .setContent(content)
            .setFinishReason(Candidate.FinishReason.OTHER)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), nullValue());
        assertThat(prediction.safetyAttributes().blocked(), is(false));
    }

    @Test
    void shouldHandleCandidateWithCitationMetadata() {
        var citation = Citation.newBuilder().setTitle("Vertex AI Documentation").build();
        var citationMetadata = CitationMetadata.newBuilder().addCitations(citation).build();
        var part = Part.newBuilder().setText("Citation test").build();
        var content = Content.newBuilder().addParts(part).build();
        var candidate = Candidate.newBuilder()
            .setContent(content)
            .setFinishReason(Candidate.FinishReason.STOP)
            .setCitationMetadata(citationMetadata)
            .build();

        var prediction = AbstractGenerativeAi.Prediction.of(candidate);

        assertThat(prediction, notNullValue());
        assertThat(prediction.content(), is("Citation test"));
        assertThat(prediction.citationMetadata().citations(), hasSize(1));
        assertThat(prediction.citationMetadata().citations().get(0).citations(), contains("Vertex AI Documentation"));
    }

    @Test
    void shouldHandleNullCandidate() {
        var prediction = AbstractGenerativeAi.Prediction.of(null);
        assertThat(prediction, nullValue());
    }

    @Test
    void shouldHandleSafetyAttributesOfWithoutFinishReason() {
        var safetyRating = SafetyRating.newBuilder()
            .setCategory(HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT)
            .setSeverityScore(0.5f)
            .setBlocked(true)
            .build();

        var attributes = AbstractGenerativeAi.SafetyAttributes.of(List.of(safetyRating));

        assertThat(attributes.blocked(), is(true));
        assertThat(attributes.categories(), contains("HARM_CATEGORY_DANGEROUS_CONTENT"));
        assertThat(attributes.scores(), contains(0.5f));
    }
}
