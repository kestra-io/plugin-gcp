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
        return new TrackingAssetEmitter(allEmitted);
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
}
