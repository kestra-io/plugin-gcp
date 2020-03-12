package org.kestra.task.gcp.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.*;
import com.google.common.collect.ImmutableMap;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Value;
import io.micronaut.test.annotation.MicronautTest;
import org.junit.jupiter.api.BeforeEach;
import org.kestra.core.runners.RunContext;
import org.kestra.core.utils.TestsUtils;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
public class ExtractToGcsTest extends AbstractBigquery{
	@Inject
	private ApplicationContext applicationContext;

	@Value("${kestra.tasks.bigquery.project}")
	private String project;

	@Value("${kestra.tasks.bigquery.dataset}")
	private String dataset;

	@Value("${kestra.tasks.bigquery.table}")
	private String table;

	@Value("${kestra.tasks.gcs.bucket}")
	private String bucket;

	@Value("${kestra.tasks.gcs.filename}")
	private String filename;

	@Value("true")
	private Boolean printHeader;

	private BigQuery connection;

	@BeforeEach
	private void init()  {
		this.connection = new Connection().of(projectId, "EU");
	}

	private Job query(String query) throws InterruptedException {
		return this.connection
				.create(JobInfo
						.newBuilder(QueryJobConfiguration.newBuilder(query).build())
						.setJobId(JobId.of(UUID.randomUUID().toString()))
						.build()
				)
				.waitFor();
	}

	@Test
	void toCsv() throws Exception {
		// Sample table
		query("CREATE OR REPLACE TABLE  `" + this.dataset + "." +  this.table + "`" +
				"(product STRING, quantity INT64)" +
				";" +
				"INSERT `" + this.dataset + "." +  this.table + "` (product, quantity)\n" +
				"VALUES('top load washer', 10),\n" +
				"      ('front load washer', 20),\n" +
				"      ('dryer', 30),\n" +
				"      ('refrigerator', 10),\n" +
				"      ('microwave', 20),\n" +
				"      ('dishwasher', 30),\n" +
				"      ('oven', 5)\n" +
				";");

		// Extract task
		ExtractToGcs task = ExtractToGcs.builder()
				.id(ExtractToGcsTest.class.getSimpleName())
				.type(ExtractToGcs.class.getName())
				.destinationUris(Collections.singletonList(
						"gs://" + this.bucket + "/" + this.filename
				))
				.sourceTable(this.project + "." + this.dataset + "." + this.table)
				.printHeader(printHeader)
				.build();

		RunContext runContext = TestsUtils.mockRunContext(applicationContext, task, ImmutableMap.of());

		ExtractToGcs.Output run = task.run(runContext);
		assertThat(run.getFileCounts().get(0), is(1L));

		// Clean sample table
		query("DROP TABLE  `" + this.dataset + "." +  this.table + "` ;");

	}
}

