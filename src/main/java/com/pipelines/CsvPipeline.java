package com.pipelines;

import java.util.ArrayList;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;

public class CsvPipeline {
	private static final Logger LOG = LoggerFactory.getLogger(CsvPipeline.class);
	
	/**
	 * The custom options supported by the pipeline. Inherits standard configuration
	 * options.
	 */
	public interface Options extends PipelineOptions {
		@Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
		@Required
		ValueProvider<String> getInputFilePattern();
		void setInputFilePattern(ValueProvider<String> value);
		
		@Description("BigQuery table (e.g. project:my_dataset.my_table)")
		@Required
		ValueProvider<String> getBigQueryTable();
		void setBigQueryTable(ValueProvider<String> value);
	}

	public static void main(String[] args) {
		LOG.info("<><> CSV pipeline");
		// Parse the user options passed from the command-line
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		run(options);	
	}
	
	/**
	 * Executes the pipeline with the provided execution parameters.
	 *
	 * @param options
	 *            The execution parameters.
	 */
	public static PipelineResult run(Options options) {
		LOG.info("<><> run(options)");
		// Create the pipeline.
		Pipeline pipeline = Pipeline.create(options);
		String bqTable = options.getBigQueryTable().toString();  //format = project:my_set.my_table
		
		/*
		 * Steps:
		 * 1. Read lines from CSV source
		 * 2. Convert CSV lines to BQ rows
		 * 3. Write BQ rows to BQ table
		 */
		PCollection<String> csvRows = pipeline.apply(TextIO.read().from(options.getInputFilePattern()));
		LOG.info("<><> csvRows.toString(): " + csvRows.toString());
		PCollection<TableRow> tableRows = csvRows.apply(ParDo.of(new CsvLineToBQRow()));
		LOG.info("<><> tableRows.toString(): " + tableRows.toString());
		
		tableRows
		.apply(
//				BigQueryIO.<TableRow> writeTableRows()
			BigQueryIO.writeTableRows()
			.to(bqTable)
			.withSchema(getTableSchema())
			.withWriteDisposition(WriteDisposition.WRITE_APPEND)
			.withCreateDisposition(CreateDisposition.CREATE_NEVER)
		);
		
		return pipeline.run();
	}

	public static class CsvLineToBQRow extends DoFn<String, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext pc) {
			String[] elements = pc.element().split(",");
			TableRow tableRow = new TableRow();
			
			LOG.info(elements.toString());
			LOG.info(tableRow.toString());
			
			tableRow.set("Member_ID", elements[0]);
			tableRow.set("First_Name", elements[1]);
			tableRow.set("Last_Name", elements[2]);
			tableRow.set("Gender", elements[3]);
			tableRow.set("Age", elements[4]);
			tableRow.set("Height", elements[5]);
			tableRow.set("Weight", elements[6]);
			tableRow.set("Hours_Sleep", elements[7]);
			tableRow.set("Calories_Consumed", elements[8]);
			tableRow.set("Exercise_Calories_Burned", elements[9]);
			tableRow.set("Date", elements[10]);
			
			pc.output(tableRow);
		}
	}
	
	private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("Member_ID").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("First_Name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Last_Name").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Gender").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Age").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("Height").setType("STRING"));
        fields.add(new TableFieldSchema().setName("Weight").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("Hours_Sleep").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("Calories_Consumed").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("Exercise_Calories_Burned").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("Date").setType("STRING"));

        return new TableSchema().setFields(fields);
    }

}
