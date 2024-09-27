@PluginSubGroup(
    title = "Dataproc Batches",
    description = "This sub-group of plugins contains tasks for submitting batches on Google Cloud Dataproc. " +
        "Dataproc is a managed Apache Spark and Apache Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning.",
    categories = { PluginSubGroup.PluginCategory.BATCH, PluginSubGroup.PluginCategory.CLOUD }
)
package io.kestra.plugin.gcp.dataproc.batches;

import io.kestra.core.models.annotations.PluginSubGroup;