package br.com.dr.leads.pipeline;

import static br.com.dr.leads.pipeline.LeadsPipeline.ERROR_TAG;
import static br.com.dr.leads.pipeline.LeadsPipeline.SUCCESS_TAG;
import static br.com.dr.leads.test.FileHelper.readFileToStream;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import br.com.dr.leads.pipeline.LeadsPipeline.ValidateEventFn;

public class ValidateEventsFnTest {


  private List<String> expectedError = readFileToStream(Paths.get("src/test/resources/leads-error.txt")).collect(Collectors.toList());

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(createOptions());

  @After
  public void cleanUp() {
    FileUtils.deleteQuietly(new File(getOptions().getOutput()));
  }

  @Test
  public void shouldProduceAFileWithInvalidDataAndAFileWithValidData() {
    PCollectionTuple output = testPipeline
        .apply(Create.of(expectedError))
        .apply(ParDo.of(new ValidateEventFn()).withOutputTags(SUCCESS_TAG, TupleTagList.of(ERROR_TAG)));

    PAssert.that(output.get(SUCCESS_TAG)).empty();
    PAssert.that(output.get(ERROR_TAG)).containsInAnyOrder(expectedError);
    testPipeline.run().waitUntilFinish();
  }

  private LeadsPipelineOptions createOptions() {
    LeadsPipelineOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(LeadsPipelineOptions.class);
    pipelineOptions.setOutput("/tmp/" + UUID.randomUUID().toString());
    return pipelineOptions;
  }

  private LeadsPipelineOptions getOptions() {
    return testPipeline.getOptions().as(LeadsPipelineOptions.class);
  }

}
