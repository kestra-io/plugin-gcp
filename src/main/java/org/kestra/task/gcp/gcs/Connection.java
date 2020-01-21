package org.kestra.task.gcp.gcs;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.kestra.task.gcp.AbstractConnection;

public class Connection extends AbstractConnection {
    public Storage of(String projectId) {
        return StorageOptions
            .newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    public Storage of(String serviceAccount, String projectId) {
        return StorageOptions
            .newBuilder()
            .setCredentials(this.credentials(serviceAccount))
            .setProjectId(projectId)
            .build()
            .getService();
    }
}
