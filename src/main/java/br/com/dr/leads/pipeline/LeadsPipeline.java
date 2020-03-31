package br.com.dr.leads.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

import br.com.dr.leads.Event;
import br.com.dr.leads.helper.FileNaming;
import com.google.gson.Gson;

import lombok.AllArgsConstructor;

public class LeadsPipeline {

  public static final TupleTag<Event> SUCCESS_TAG = new TupleTag<Event>() {
  };
  public static final TupleTag<String> ERROR_TAG = new TupleTag<String>() {
  };

  public static void main(String[] args) {
    PipelineOptionsFactory.register(LeadsPipelineOptions.class);
    LeadsPipelineOptions options = PipelineOptionsFactory.fromArgs(args).create().as(LeadsPipelineOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("ReadData", PubsubIO.readStrings().fromSubscription(options.getSubscription()))
        .apply("ProcessEvent", new ProcessEvent(options.getWindowInSeconds(), options.getShardsNum(), options.getOutput()));

    pipeline.run();
  }

  @AllArgsConstructor
  public static class ProcessEvent extends PTransform<PCollection<String>, PDone> {

    private long windowInSeconds;
    private int shardsNum;
    private String outputPath;

    @Override
    public PDone expand(PCollection<String> input) {
      PCollectionTuple pCollectionTuple = input
          .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))))
          .apply("ValidateEvent", ParDo.of(new ValidateEvent()).withOutputTags(SUCCESS_TAG, TupleTagList.of(ERROR_TAG)));

      pCollectionTuple
          .get(SUCCESS_TAG)
          .apply(MapElements.into(TypeDescriptors.strings()).via(e -> new Gson().toJson(e)))
          .apply("WriteSuccess",
              FileIO.<String>write()
                  .withNumShards(shardsNum)
                  .withNaming(new FileNaming())
                  .via(Contextful.fn(event -> event), TextIO.sink())
                  .to(outputPath + "/success/"));

      pCollectionTuple
          .get(ERROR_TAG)
          .apply("WriteError",
              FileIO.<String>write()
                  .withNumShards(shardsNum)
                  .withNaming(new FileNaming())
                  .via(Contextful.fn(event -> event), TextIO.sink())
                  .to(outputPath + "/error/"));

      return PDone.in(input.getPipeline());
    }
  }

  public static class ValidateEvent extends DoFn<String, Event> {

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
}
