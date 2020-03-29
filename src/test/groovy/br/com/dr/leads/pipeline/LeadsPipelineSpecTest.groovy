package br.com.dr.leads.pipeline

import br.com.dr.leads.pipeline.LeadsPipeline
import br.com.dr.leads.pipeline.LeadsPipelineOptions
import groovy.io.FileType
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.commons.io.FileUtils
import org.junit.Rule
import spock.lang.Shared
import spock.lang.Specification
import sun.security.provider.SHA

class LeadsPipelineSpecTest extends Specification {

  def final EXPECTED_SHARDS = 2

  @Rule
  public transient TestPipeline testPipeline = TestPipeline.fromOptions(createOptions())
  @Shared
  def expectedError
  @Shared
  def expectedSuccess
  @Shared
  def expectedAllData

  def setupSpec() {
    expectedError = new File('src/test/resources/leads-error.txt').readLines()
    expectedSuccess = new File('src/test/resources/leads-success.txt').readLines()
    expectedAllData = expectedError + expectedSuccess
  }

  def cleanup() {
    FileUtils.deleteDirectory(new File(testPipeline.getOptions().output))
  }

  def 'should produce a file with invalid data and a file with valid data'() {
    given: 'a pipeline with valid and invalid data'
    testPipeline
        .apply(Create.of(expectedAllData))
        .apply(new LeadsPipeline.ProcessEvent(testPipeline.getOptions().getWindowInSeconds(),
            testPipeline.getOptions().getShardsNum(), testPipeline.getOptions().getOutput()))

    when: 'the pipeline run'
    testPipeline.run().waitUntilFinish()

    then: 'should be created a file with invalid data'
    def actualErrorData = readOutputFiles('error/')
    expectedError.size() == actualErrorData.size()
    expectedError.containsAll(actualErrorData)

    and: 'should be created a file with valid data'
    def actualSuccessData = readOutputFiles('success/')
    expectedSuccess.size() == actualSuccessData.size()
    expectedSuccess.containsAll(actualSuccessData)
  }

  def 'should produce two shards for each type of file'() {
    given: 'a pipeline with valid and invalid data'
    testPipeline
        .apply(Create.of(expectedAllData))
        .apply(new LeadsPipeline.ProcessEvent(testPipeline.getOptions().getWindowInSeconds(),
            testPipeline.getOptions().getShardsNum(), testPipeline.getOptions().getOutput()))

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
