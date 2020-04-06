package br.com.dr.leads.pipeline;

import static br.com.dr.leads.pipeline.LeadsPipeline.CSV_TAG;
import static br.com.dr.leads.test.FileHelper.getNumberOfShardsProduced;
import static br.com.dr.leads.test.FileHelper.readFileToStream;
import static br.com.dr.leads.test.FileHelper.readOutput;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import br.com.dr.leads.pipeline.LeadsPipeline.ReadCsv;
import br.com.dr.leads.pipeline.LeadsPipeline.ProcessEvent;

public class LeadsPipelineTest {

  private static final int EXPECTED_SHARDS = 2;

  private List<String> inputError = readFileToStream(Paths.get("src/test/resources/leads-error.txt")).collect(Collectors.toList());
  private List<String> inputSuccess = readFileToStream(Paths.get("src/test/resources/leads-success.txt")).collect(Collectors.toList());
  private List<String> expectedSuccess = readFileToStream(Paths.get("src/test/resources/leads-success-with-average-salary.txt"))
      .collect(Collectors.toList());
  private List<String> inputAllData = new ArrayList<>(CollectionUtils.union(inputError, inputSuccess));
  private TestStream<String> testInputStream = TestStream.create(StringUtf8Coder.of())
      .addElements(inputAllData.get(0), inputAllData.subList(1, inputAllData.size()).toArray(new String[]{}))
      .advanceWatermarkToInfinity();

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(createOptions());

  @After
  public void cleanUp() {
    FileUtils.deleteQuietly(new File(getOptions().getOutput()));
  }

  @Test
  public void shouldProduceAFileWithInvalidDataAndAFileWithValidData() throws Exception {
    PCollection<String> pubSubData = testPipeline.apply(testInputStream);
    PCollection<Row> csvData = testPipeline.apply("ProcessJobTitlesCsv", new ReadCsv(getOptions().getJobTitlesCsvPath()));

    PCollectionTuple.of(LeadsPipeline.EVENT_TAG, pubSubData)
        .and(CSV_TAG, csvData)
        .apply("ProcessEvent", new ProcessEvent(getOptions().getWindowInSeconds(), getOptions().getShardsNum(), getOptions().getOutput()));

    testPipeline.run().waitUntilFinish();
    assertData(getOptions().getOutput() + "/error/", inputError);
    assertData(getOptions().getOutput() + "/success/", expectedSuccess);
  }

  @Test
  public void shouldProduce2Shards() throws Exception {
    PCollection<String> pubSubData = testPipeline.apply(testInputStream);
    PCollection<Row> csvData = testPipeline.apply("ProcessJobTitlesCsv", new ReadCsv(getOptions().getJobTitlesCsvPath()));

    PCollectionTuple.of(LeadsPipeline.EVENT_TAG, pubSubData)
        .and(CSV_TAG, csvData)
        .apply("ProcessEvent", new ProcessEvent(getOptions().getWindowInSeconds(), getOptions().getShardsNum(), getOptions().getOutput()));

    testPipeline.run().waitUntilFinish();

    Assert.assertEquals(EXPECTED_SHARDS, getNumberOfShardsProduced(getOptions().getOutput() + "/error"));
    Assert.assertEquals(EXPECTED_SHARDS, getNumberOfShardsProduced(getOptions().getOutput() + "/success"));
  }

  private LeadsPipelineOptions createOptions() {
    LeadsPipelineOptions pipelineOptions = TestPipeline.testingPipelineOptions().as(LeadsPipelineOptions.class);
    pipelineOptions.setOutput("/tmp/" + UUID.randomUUID().toString());
    pipelineOptions.setShardsNum(EXPECTED_SHARDS);
    pipelineOptions.setJobTitlesCsvPath("src/test/resources/job-titles.csv");
    return pipelineOptions;
  }

  private LeadsPipelineOptions getOptions() {
    return testPipeline.getOptions().as(LeadsPipelineOptions.class);
  }

  private void assertData(String path, Collection<String> expectedData) throws IOException {
    Collection<String> actualData = readOutput(path);
    Assert.assertEquals(expectedData.size(), actualData.size());
    Assert.assertTrue(expectedData.containsAll(actualData));
  }

}
