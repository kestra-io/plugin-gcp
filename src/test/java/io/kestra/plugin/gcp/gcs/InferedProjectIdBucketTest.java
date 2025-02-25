package io.kestra.plugin.gcp.gcs;

public class InferedProjectIdBucketTest extends AbstractBucketTest {
    @Override
    public boolean inferProjectId() {
        return true;
    }
}
