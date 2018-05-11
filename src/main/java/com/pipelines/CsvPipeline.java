package com.pipelines;

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

import com.google.api.services.bigquery.model.TableRow;

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
	}

	public static void main(String[] args) {
		
		System.out.println("CSV pipeline");
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
		// Create the pipeline.
		Pipeline pipeline = Pipeline.create(options);
		String bqTable = "";  //format = project:my_set.my_table
		
		/*
		 * Steps:
		 * 1. Read lines from CSV source
		 * 2. Convert CSV lines to BQ rows
		 * 3. Write BQ rows to BQ table
		 */
		PCollection<String> csvRows = pipeline.apply(TextIO.read().from(options.getInputFilePattern()));
		PCollection<TableRow> tableRows = csvRows.apply(ParDo.of(new CsvLineToBQRow()));
		
		tableRows
		.apply(BigQueryIO.<TableRow> writeTableRows()
			.to(bqTable)
			// .withSchema(getSchema())
			.withWriteDisposition(WriteDisposition.WRITE_APPEND)
			.withCreateDisposition(CreateDisposition.CREATE_NEVER));
		
		return pipeline.run();
	}

	public static class CsvLineToBQRow extends DoFn<String, TableRow> {
		@ProcessElement
		public void processElement(ProcessContext pc) {
			String[] elements = pc.element().split(",");
			TableRow tableRow = new TableRow();
			
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

}
