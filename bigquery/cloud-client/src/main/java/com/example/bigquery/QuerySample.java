/*
  Copyright 2016, Google, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.example.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.QueryResult;
import com.google.cloud.bigquery.TableId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/** Runs a query against BigQuery. */
public class QuerySample {
  private static final String DEFAULT_QUERY =
      "SELECT corpus FROM `publicdata.samples.shakespeare` GROUP BY corpus;";
  private static final long TEN_SECONDS_MILLIS = 10000;


  /**
   * Perform the given query using the synchronous api.
   *
   * @param out stream to write results to
   * @param queryString query to run
   * @param useLegacySql Boolean that is false if using standard SQL syntax.
   */
  // [START run]
  public static void run(
      final PrintStream out,
      final String queryString,
      final @Nullable String destinationDataset,
      final @Nullable String destinationTable,
      final boolean useLegacySql,
      final boolean allowLargeResults)
      throws IOException, InterruptedException, TimeoutException {
    BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    // [START query_config]
    QueryJobConfiguration.Builder queryConfigBuilder =
        QueryJobConfiguration.newBuilder(queryString)
            // To use standard SQL syntax, set useLegacySql to false.
            // See: https://cloud.google.com/bigquery/sql-reference/
            .setUseLegacySql(useLegacySql)
            // Allow results larger than the maximum response size.
            // If true, a destination table must be set.
            // See: https://cloud.google.com/bigquery/querying-data#large-results
            .setAllowLargeResults(allowLargeResults);

    if (destinationDataset != null && destinationTable != null) {
      // Save the results of the query to a permanent table.
      // See: https://cloud.google.com/bigquery/querying-data#permanent-table
      queryConfigBuilder.setDestinationTable(TableId.of(destinationDataset, destinationTable));
    }
    QueryJobConfiguration queryConfig = queryConfigBuilder.build();
    // [END query_config]

    // Create a job ID so that we can safely retry.
    JobId jobId = JobId.of(UUID.randomUUID().toString());
    Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

    // Wait for the query to complete.
    queryJob = queryJob.waitFor();

    // Check for errors
    if (queryJob == null) {
      throw new RuntimeException("Job no longer exists");
    } else if (queryJob.getStatus().getError() != null) {
      // You can also look at queryJob.getStatus().getExecutionErrors() for all
      // errors, not just the latest one.
      throw new RuntimeException(queryJob.getStatus().getError().toString());
    }

    // Get the results.
    QueryResponse response = bigquery.getQueryResults(jobId);
    QueryResult result = response.getResult();

    while (result != null) {
      if (response.hasErrors()) {
        String firstError = "";
        if (response.getExecutionErrors().size() != 0) {
          firstError = response.getExecutionErrors().get(0).getMessage();
        }
        throw new RuntimeException(firstError);
      }

      Iterator<List<FieldValue>> iter = result.iterateAll();
      while (iter.hasNext()) {
        List<FieldValue> row = iter.next();
        for (FieldValue val : row) {
          out.printf("%s,", val.toString());
        }
        out.printf("\n");
      }

      result = result.getNextPage();
    }
  }
  // [END run]

  /** Prompts the user for the required parameters to perform a query. */
  public static void main(final String[] args)
      throws IOException, InterruptedException, TimeoutException {
    String queryString = System.getProperty("query");
    if (queryString == null || queryString.isEmpty()) {
      System.out.println("The query property was not set, using default.");
      queryString = DEFAULT_QUERY;
    }
    System.out.printf("query: %s\n", queryString);

    String useLegacySqlString = System.getProperty("useLegacySql");
    if (useLegacySqlString == null || useLegacySqlString.isEmpty()) {
      useLegacySqlString = "false";
    }
    boolean useLegacySql = Boolean.parseBoolean(useLegacySqlString);

    run(System.out, queryString, useLegacySql);
  }
}
