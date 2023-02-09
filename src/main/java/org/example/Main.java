package org.example;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;

public class Main {
    public static void main(String[] args) {

        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<TableRow> tableA = pipeline.apply("Read from BigQuery table A",
                BigQueryIO.readTableRows().withoutValidation().fromQuery(
                        "SELECT * FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations` LIMIT 10;"));

        PCollection<TableRow> tableB = pipeline.apply("Read from BigQuery table B",
                BigQueryIO.readTableRows().withoutValidation().fromQuery(
                        "SELECT * FROM `bigquery-public-data.san_francisco.bikeshare_stations` LIMIT 10;"));

        PCollection<TableRow> tableC = pipeline.apply("Read from BigQuery table C",
                BigQueryIO.readTableRows().withoutValidation().fromQuery(
                        "SELECT * FROM `bigquery-public-data.new_york_citibike.citibike_stations` LIMIT 10;"));

        // Flatten takes a PCollectionList of PCollection objects of a given type.
        // Returns a single PCollection that contains all of the elements in the PCollection objects in that list.
        PCollectionList<TableRow> collections = PCollectionList
                .of(tableA).and(tableB).and(tableC);

        PCollection<TableRow> mergedTables = collections.apply(Flatten.pCollections());

        // Maps records to strings
        mergedTables.apply(MapElements
                .into(TypeDescriptors.strings())
                .via(record -> record.toString())
        )// Writes each window of records into GCS
                .apply(TextIO
                        .write()
                        .to("merged_tables.txt")
                        .withNumShards(1)
                );

        // Select final schema

        // Write to BQ

        pipeline.run().waitUntilFinish();
    }
}