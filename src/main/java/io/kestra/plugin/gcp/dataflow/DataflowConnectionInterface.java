package io.kestra.plugin.gcp.dataflow;

import java.util.List;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.gcp.GcpInterface;

public interface DataflowConnectionInterface extends GcpInterface {
    Property<String> getProjectId();

    Property<String> getServiceAccount();

    Property<String> getImpersonatedServiceAccount();

    Property<List<String>> getScopes();

    Property<String> getLocation();
}
