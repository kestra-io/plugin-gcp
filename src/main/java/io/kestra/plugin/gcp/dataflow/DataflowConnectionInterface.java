package io.kestra.plugin.gcp.dataflow;

import io.kestra.core.models.property.Property;
import io.kestra.plugin.gcp.GcpInterface;

public interface DataflowConnectionInterface extends GcpInterface {
    Property<String> getLocation();
}
