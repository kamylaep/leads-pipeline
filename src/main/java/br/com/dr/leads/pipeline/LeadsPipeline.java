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
      TupleTag<Event> successTag = new TupleTag<Event>() {
      };
      TupleTag<String> errorTag = new TupleTag<String>() {
      };

      PCollectionTuple pCollectionTuple = input
          .apply("Window", Window.into(FixedWindows.of(Duration.standardSeconds(windowInSeconds))))
          .apply("ValidateEvent", ParDo.of(new ValidateEvent(errorTag)).withOutputTags(successTag, TupleTagList.of(errorTag)));

      pCollectionTuple
          .get(successTag)
          .apply(MapElements.into(TypeDescriptors.strings()).via(e -> new Gson().toJson(e)))
          .apply("WriteSuccess",
              FileIO.<String>write()
                  .withNumShards(shardsNum)
                  .withNaming(new FileNaming())
                  .via(Contextful.fn(event -> event), TextIO.sink())
                  .to(outputPath + "/success/"));

      pCollectionTuple
          .get(errorTag)
          .apply("WriteError",
              FileIO.<String>write()
                  .withNumShards(shardsNum)
                  .withNaming(new FileNaming())
                  .via(Contextful.fn(event -> event), TextIO.sink())
                  .to(outputPath + "/error/"));

      return PDone.in(input.getPipeline());
    }
  }

  @AllArgsConstructor
  public static class ValidateEvent extends DoFn<String, Event> {

    private TupleTag<String> errorTag;

    @ProcessElement
    public void processElement(ProcessContext context) {
      Gson gson = new Gson();
      String json = context.element();
      try {
        Event event = gson.fromJson(json, Event.class);

        if (event.isValid()) {
          context.output(event);
        } else {
          context.output(errorTag, json);
        }
      } catch (Exception e) {
        context.output(errorTag, json);
      }
    }
  }
}
