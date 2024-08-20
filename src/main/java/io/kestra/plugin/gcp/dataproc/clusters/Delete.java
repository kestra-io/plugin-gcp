package io.kestra.plugin.gcp.dataproc.clusters;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.ClusterControllerClient;
import com.google.cloud.dataproc.v1.ClusterControllerSettings;
import com.google.cloud.dataproc.v1.ClusterOperationMetadata;
import com.google.cloud.dataproc.v1.DeleteClusterRequest;
import com.google.protobuf.Empty;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.gcp.AbstractTask;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
 title = "Delete clusters from Google Cloud Dataproc."
)
@Plugin(
	examples = @Example(
		title = "Deletes a cluster from Google Cloud Dataproc.",
		code = {
            "clusterName: YOUR_CLUSTER_NAME",
            "region: YOUR_REGION",
		}
	)
)
public class Delete extends AbstractTask implements RunnableTask<Delete.Output> {

	public static final String DATAPROC_GOOGLEAPIS = "-dataproc.googleapis.com:443";

	@Schema(
		title = "The cluster name."
	)
	@NotBlank
	@PluginProperty(dynamic = true)
	private String clusterName;

	@Schema(
		title = "The region."
	)
	@NotBlank
	@PluginProperty(dynamic = true)
	private String region;

	@Override
	public Output run(RunContext runContext) throws Exception {
		String region = runContext.render(this.region);
		String clusterName = runContext.render(this.clusterName);

		ClusterControllerSettings settings = ClusterControllerSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(this.credentials(runContext)))
			.setEndpoint(region + DATAPROC_GOOGLEAPIS)
			.build();

		try (ClusterControllerClient client = ClusterControllerClient.create(settings)) {
			DeleteClusterRequest request = DeleteClusterRequest.newBuilder()
				.setProjectId(runContext.render(this.projectId))
				.setRegion(region)
				.setClusterName(clusterName)
				.build();

			OperationFuture<Empty, ClusterOperationMetadata> clusterAsync = client.deleteClusterAsync(request);
			clusterAsync.get();
			ClusterOperationMetadata metadata = clusterAsync.getMetadata().get();

			if (metadata == null) {
				return Output.builder()
					.clusterName(this.clusterName)
					.deleted(true)
					.build();
			}

			return Output.builder()
				.clusterName(metadata.getClusterName())
				.deleted(metadata.getStatus().getInnerState().equalsIgnoreCase("DONE"))
				.build();
		}
	}

	@Getter
	@Builder
	public static class Output implements io.kestra.core.models.tasks.Output {

		@Schema(
			title = "The cluster name."
		)
		private String clusterName;

		@Schema(
			title = "Whether cluster was deleted successfully."
		)
		private boolean deleted;

	}

}
