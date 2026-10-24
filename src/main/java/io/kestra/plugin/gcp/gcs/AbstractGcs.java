package io.kestra.plugin.gcp.gcs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.shared.AbstractTask;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractGcs extends AbstractTask {
    private static final Pattern SCHEME_AUTH_PATTERN = Pattern.compile("^([a-zA-Z][a-zA-Z0-9+.-]*://[^/]*)(/.*)?$");

    Storage connection(RunContext runContext) throws IOException, IllegalVariableEvaluationException {
        // Resolve credentials before projectId because credentials may infer it from the service account.
        return StorageOptions.newBuilder()
            .setCredentials(this.credentials(runContext))
            .setProjectId(runContext.render(projectId).as(String.class).orElse(null))
            .setHeaderProvider(() -> Map.of("user-agent", "Kestra/" + runContext.version()))
            .build()
            .getService();
    }

    static URI encode(RunContext runContext, String blob) throws IllegalVariableEvaluationException, URISyntaxException {
        return new URI(encode(runContext.render(blob)));
    }

    static String encode(String blob) {
        if (blob == null) {
            return null;
        }

        var matcher = SCHEME_AUTH_PATTERN.matcher(blob);
        if (matcher.matches()) {
            var prefix = matcher.group(1);
            var path = matcher.group(2) != null ? matcher.group(2) : "";
            return prefix + encodePath(path);
        }

        return encodePath(blob);
    }

    public static String encodePath(String path) {
        if (path == null) {
            return null;
        }

        var sb = new StringBuilder(path.length());
        for (var i = 0; i < path.length();) {
            var cp = path.codePointAt(i);
            if (cp < 128 && isUnreservedOrSubDelimOrSlash((char) cp)) {
                sb.append((char) cp);
            } else if (cp == '%') {
                if (i + 2 < path.length() && isHex(path.charAt(i + 1)) && isHex(path.charAt(i + 2))) {
                    sb.append('%');
                    sb.append(path.charAt(i + 1));
                    sb.append(path.charAt(i + 2));
                    i += 3;
                    continue;
                } else {
                    sb.append("%25");
                }
            } else {
                var bytes = new String(Character.toChars(cp)).getBytes(StandardCharsets.UTF_8);
                for (var b : bytes) {
                    sb.append(String.format("%%%02X", b));
                }
            }
            i += Character.charCount(cp);
        }
        return sb.toString();
    }

    private static boolean isHex(char c) {
        return (c >= '0' && c <= '9') ||
            (c >= 'a' && c <= 'f') ||
            (c >= 'A' && c <= 'F');
    }

    private static boolean isUnreservedOrSubDelimOrSlash(char c) {
        return (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') ||
            c == '-' || c == '.' || c == '_' || c == '~' ||
            c == '!' || c == '$' || c == '&' || c == '\'' ||
            c == '(' || c == ')' || c == '*' || c == '+' ||
            c == ',' || c == ';' || c == '=' || c == ':' ||
            c == '@' || c == '/';
    }

    static String blobPath(String path) {
        return path.replace("+", " ");
    }
}
