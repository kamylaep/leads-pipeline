package br.com.dr.leads.pipeline


import groovy.io.FileType
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.apache.commons.io.FileUtils
import org.junit.Rule
import spock.lang.Shared
import spock.lang.Specification

class LeadsPipelineSpecTest extends Specification {

  def final EXPECTED_SHARDS = 2

  @Rule
  public transient TestPipeline testPipeline = TestPipeline.fromOptions(createOptions())
  @Shared
  def inputError = new File('src/test/resources/leads-error.txt').readLines()
  @Shared
  def inputSuccess = new File('src/test/resources/leads-success.txt').readLines()
  @Shared
  def inputAllData = inputError + inputSuccess
  @Shared
  def expectedSuccess = new File('src/test/resources/leads-success-with-average-salary.txt').readLines()
  @Shared
  def testInputStream = TestStream.create(StringUtf8Coder.of())
      .addElements(inputAllData[0], *inputAllData.subList(1, inputAllData.size()))
      .advanceWatermarkToInfinity()

  def cleanup() {
    FileUtils.deleteDirectory(new File(testPipeline.getOptions().output))
  }

  def 'should produce a file with invalid data and a file with valid data'() {
    given: 'a pipeline with valid and invalid data'
    testPipeline
        .apply(testInputStream)
        .apply(new LeadsPipeline.ProcessEvent(testPipeline.getOptions().getWindowInSeconds(), testPipeline.getOptions().getShardsNum(),
            testPipeline.getOptions().getOutput(), testPipeline.getOptions().getJobTitlesCsvPath()))

    when: 'the pipeline run'
    testPipeline.run().waitUntilFinish()

    then: 'should be created a file with invalid data'
    def actualErrorData = readOutputFiles('error/')
    inputError.size() == actualErrorData.size()
    inputError.containsAll(actualErrorData)

    and: 'should be created a file with valid data'
    def actualSuccessData = readOutputFiles('success/')
    expectedSuccess.size() == actualSuccessData.size()
    expectedSuccess.containsAll(actualSuccessData)
  }

  def 'should produce two shards for each type of file'() {
    given: 'a pipeline with valid and invalid data'
    testPipeline
        .apply(testInputStream)
        .apply(new LeadsPipeline.ProcessEvent(testPipeline.getOptions().getWindowInSeconds(), testPipeline.getOptions().getShardsNum(),
            testPipeline.getOptions().getOutput(), testPipeline.getOptions().getJobTitlesCsvPath()))

    when: 'the pipeline run'
    testPipeline.run().waitUntilFinish()

    then: 'should be created 2 shards for the error files'
    EXPECTED_SHARDS == readShards('error/')

    and: 'should be created 2 shards for the success files'
    EXPECTED_SHARDS == readShards('success/')
  }

  def createOptions() {
    def pipelineOptions = TestPipeline.testingPipelineOptions().as(LeadsPipelineOptions.class)
    pipelineOptions.output = "/tmp/${UUID.randomUUID().toString()}"
    pipelineOptions.shardsNum = EXPECTED_SHARDS
    pipelineOptions.jobTitlesCsvPath = 'src/test/resources/job-titles.csv'
    return pipelineOptions
  }

  def readOutputFiles(path) {
    def content = []
    new File("${testPipeline.getOptions().output}/$path")
        .traverse(type: FileType.FILES, visit: { content.push(it.readLines()) })
    return content.flatten()
  }

  def readShards(path) {
    def shards = 0
    new File("${testPipeline.getOptions().output}/$path")
        .traverse(type: FileType.FILES, visit: { shards++ })
    return shards
  }
}
