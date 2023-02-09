package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class Main {
    public static void main(String[] args) {

        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> tableA = pipeline
                .apply(
                        "Read from BigQuery table A",
                        BigQueryIO.readTableRows().withoutValidation().from(""));

        PCollection<TableRow> tableB = pipeline
                .apply(
                        "Read from BigQuery table B",
                        BigQueryIO.readTableRows().withoutValidation().from(""));

        PCollection<TableRow> tableC = pipeline
                .apply(
                        "Read from BigQuery table C",
                        BigQueryIO.readTableRows().withoutValidation().from(""));

        // Flatten takes a PCollectionList of PCollection objects of a given type.
        // Returns a single PCollection that contains all of the elements in the PCollection objects in that list.
        PCollectionList<TableRow> collections = PCollectionList
                .of(tableA).and(tableB).and(tableC);

        PCollection<TableRow> mergedTables = collections.apply(Flatten.pCollections());

        // Select final schema

        // Write to BQ

        pipeline.run().waitUntilFinish();
    }
}