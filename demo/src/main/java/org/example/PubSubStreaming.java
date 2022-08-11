package org.example;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


public class PubSubStreaming {
    public static void main( String[] args )
    {
        DataflowPipelineOptions dataflowPipelineOptions= PipelineOptionsFactory.as(DataflowPipelineOptions.class);
        dataflowPipelineOptions.setJobName("usecase1-labid-5");
        dataflowPipelineOptions.setProject("nttdata-c4e-bde");
        dataflowPipelineOptions.setRegion("europe-west4");
        dataflowPipelineOptions.setGcpTempLocation("gs://c4e-uc1-dataflow-temp-5/temp");
        dataflowPipelineOptions.setRunner(DataflowRunner.class);

        Pipeline p = Pipeline.create(dataflowPipelineOptions);

        PCollection<String> pubsubmessage= p.apply(PubsubIO.readStrings().fromTopic("projects/nttdata-c4e-bde/topics/uc1-input-topic-5"));
        PCollection<TableRow> bigqueryrow= pubsubmessage.apply(ParDo.of(new ConverterBigqueryStr()));

        bigqueryrow.apply(BigQueryIO.writeTableRows().to("nttdata-c4e-bde.uc1_5.account")
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        p.run();




    }

    public static class ConverterBigqueryStr extends org.apache.beam.sdk.transforms.DoFn<String, TableRow> {

        @ProcessElement
        public void processing(ProcessContext processContext){

            TableRow tableRow = new TableRow().set("id",processContext.toString())
                    .set("name",processContext.toString())
                    .set("surname",processContext.toString());

            processContext.output(tableRow);
        }
    }
}