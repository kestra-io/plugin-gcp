package io.kestra.plugin.gcp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.kestra.core.assets.AssetManagerFactory;
import io.kestra.core.runners.AssetEmit;
import io.kestra.core.runners.AssetEmitter;

import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Singleton;

@Singleton
@Replaces(AssetManagerFactory.class)
public class TestAssetManagerFactory extends AssetManagerFactory {
    private final List<AssetEmit> allEmitted = Collections.synchronizedList(new ArrayList<>());

    @Override
    public AssetEmitter of(boolean enable) {
        // Mirrors EE's InMemoryAssetEmitter: when the task/trigger's assets.enableAuto is false, emission
        // must be a silent no-op, not just untracked, so tests exercise the same enable/disable contract.
        return enable ? new TrackingAssetEmitter(allEmitted) : new NoopAssetEmitter();
    }

    /** All assets emitted across all RunContexts (for runner/integration tests). */
    public List<AssetEmit> allEmitted() {
        return List.copyOf(allEmitted);
    }

    public void clear() {
        allEmitted.clear();
    }

    private static final class TrackingAssetEmitter implements AssetEmitter {
        private final List<AssetEmit> shared;
        private final List<AssetEmit> local = new ArrayList<>();

        TrackingAssetEmitter(List<AssetEmit> shared) {
            this.shared = shared;
        }

        @Override
        public void emit(AssetEmit assetEmit) {
            local.add(assetEmit);
            shared.add(assetEmit);
        }

        @Override
        public List<AssetEmit> emitted() {
            return List.copyOf(local);
        }
    }

    private static final class NoopAssetEmitter implements AssetEmitter {
        @Override
        public void emit(AssetEmit assetEmit) {
            // no-op: matches EE's InMemoryAssetEmitter(enabled = false) behavior.
        }

        @Override
        public List<AssetEmit> emitted() {
            return List.of();
        }
    }
}
