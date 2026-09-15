package io.kestra.plugin.gcp;

import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @deprecated moved to {@link io.kestra.plugin.gcp.shared.AbstractTask}; kept for backward compatibility, will be removed in the next major.
 */
@Deprecated(since = "2.12.0", forRemoval = true)
@SuppressWarnings("removal")
@SuperBuilder
@NoArgsConstructor
public abstract class AbstractTask extends io.kestra.plugin.gcp.shared.AbstractTask implements GcpInterface {
}
