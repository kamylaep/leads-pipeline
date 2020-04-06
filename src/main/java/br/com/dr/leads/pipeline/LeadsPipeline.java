package br.com.dr.leads.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import br.com.dr.leads.Event;
import br.com.dr.leads.EventData;
import br.com.dr.leads.helper.FileNaming;
import com.google.gson.Gson;

import lombok.AllArgsConstructor;

public class LeadsPipeline {

  public static final TupleTag<Event> SUCCESS_TAG = new TupleTag<Event>() {
  };
  public static final TupleTag<String> ERROR_TAG = new TupleTag<String>() {
  };
  public static final TupleTag<String> EVENT_TAG = new TupleTag<String>() {
  };
  public static final TupleTag<Row> CSV_TAG = new TupleTag<Row>() {
  };

  public static final Schema EVENT_SCHEMA = Schema.builder()
      .addStringField("eventTimestamp")
      .addStringField("type")
      .addNullableField("id", FieldType.INT64)
      .addNullableField("name", FieldType.STRING)
      .addNullableField("jobTitle", FieldType.STRING)
      .build();

  public static final Schema JOB_TITLES_SCHEMA = Schema.builder()
      .addStringField("description")
      .addDoubleField("averageSalary")
      .build();

  public static void main(String[] args) {
    PipelineOptionsFactory.register(LeadsPipelineOptions.class);
    LeadsPipelineOptions options = PipelineOptionsFactory.fromArgs(args).create().as(LeadsPipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> pubSubData = pipeline
        .apply("ReadPubSub", PubsubIO.readStrings().fromSubscription(options.getSubscription()));

    PCollection<Row> csvData = pipeline
        .apply("ProcessJobTitlesCsv", new ReadCsv(options.getJobTitlesCsvPath()));

    PCollectionTuple.of(EVENT_TAG, pubSubData)
        .and(CSV_TAG, csvData)
        .apply("ProcessEvent", new ProcessEvent(options.getWindowInSeconds(), options.getShardsNum(), options.getOutput()));

    pipeline.run();
  }

  @AllArgsConstructor
  public static class ReadCsv extends PTransform<PBegin, PCollection<Row>> {

    private String jobTitlesCsvPath;

    @Override
    public PCollection<Row> expand(PBegin input) {
      return input.apply("ReadJobsTitlesCsv", TextIO.read().from(jobTitlesCsvPath))
          .apply("JobTitlesToRow", MapElements.into(TypeDescriptors.rows())
              .via(c -> {
                String[] csvRow = c.split(",");
                return Row.withSchema(JOB_TITLES_SCHEMA)
                    .addValues(csvRow[0], Double.parseDouble(csvRow[1]))
                    .build();
              })).setRowSchema(JOB_TITLES_SCHEMA);
    }
  }

  @AllArgsConstructor
  public static class ProcessEvent extends PTransform<PCollectionTuple, PDone> {

    private long windowInSeconds;
    private int shardsNum;
    private String outputPath;

    @Override
    public PDone expand(PCollectionTuple leadsAndJobTitlesTuple) {

      PCollectionTuple validEvents = leadsAndJobTitlesTuple
          .get(EVENT_TAG)
          .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))))
          .apply("ValidateEvent", ParDo.of(new ValidateEventFn())
              .withOutputTags(SUCCESS_TAG, TupleTagList.of(ERROR_TAG)));

      PCollection<Row> eventsRows = validEvents
          .get(SUCCESS_TAG)
          .apply("EventToRows", MapElements.into(TypeDescriptors.rows())
              .via(c -> Row.withSchema(EVENT_SCHEMA)
                  .addValues(c.getTimestamp(), c.getType(), c.getData().getId(), c.getData().getName(), c.getData().getJobTitle())
                  .build())).setRowSchema(EVENT_SCHEMA);

      PCollection<Row> csvRows = leadsAndJobTitlesTuple.get(CSV_TAG)
          .apply("CsvWindow", Window.into(new GlobalWindows()));

      PCollectionTuple.of("Events", eventsRows).and("JobTitles", csvRows)
          .apply("JoinEventsAndJobs",
              SqlTransform.query(
                  "SELECT Events.eventTimestamp, Events.type, Events.id, Events.name, Events.jobTitle, JobTitles.averageSalary"
                      + " FROM Events LEFT OUTER JOIN JobTitles ON Events.jobTitle = JobTitles.description"))
          .apply("RowToEvent", ParDo.of(new RowEventFn()))
          .apply("EventsToJson",
              MapElements.into(TypeDescriptors.strings()).via(e -> new Gson().toJson(e)))
          .apply("WriteSuccess",
              FileIO.<String>write()
                  .withNumShards(shardsNum)
                  .withNaming(new FileNaming())
                  .via(Contextful.fn(event -> event), TextIO.sink())
                  .to(outputPath + "/success/"));

      validEvents
          .get(ERROR_TAG)
          .apply("WriteError",
              FileIO.<String>write()
                  .withNumShards(shardsNum)
                  .withNaming(new FileNaming())
                  .via(Contextful.fn(event -> event), TextIO.sink())
                  .to(outputPath + "/error/"));

      return PDone.in(leadsAndJobTitlesTuple.getPipeline());
    }
  }

  public static class ValidateEventFn extends DoFn<String, Event> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Gson gson = new Gson();
      String json = context.element();
      try {
        Event event = gson.fromJson(json, Event.class);

        if (event.isValid()) {
          context.output(SUCCESS_TAG, event);
        } else {
          context.output(ERROR_TAG, json);
        }
      } catch (Exception e) {
        context.output(ERROR_TAG, json);
      }
    }
  }

  public static class RowEventFn extends DoFn<Row, Event> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      Row row = context.element();
      EventData eventData =
          EventData.create(row.getInt64("id"), row.getString("name"), row.getString("jobTitle"), row.getDouble("averageSalary"));
      Event event = Event.create(row.getString("eventTimestamp"), row.getString("type"), eventData);
      context.output(event);
    }
  }
}
