package br.com.dr.leads.pipeline

import br.com.dr.leads.Event
import br.com.dr.leads.pipeline.LeadsPipeline
import br.com.dr.leads.pipeline.LeadsPipelineOptions

import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList
import org.apache.commons.io.FileUtils
import org.junit.Rule
import spock.lang.Specification
import spock.lang.Unroll

import static br.com.dr.leads.pipeline.LeadsPipeline.ERROR_TAG
import static br.com.dr.leads.pipeline.LeadsPipeline.SUCCESS_TAG

class ValidateEventSpecTest extends Specification implements Serializable {

  @Rule
  public final transient TestPipeline testPipeline = TestPipeline.fromOptions(createOptions())

  def cleanup() {
    FileUtils.deleteDirectory(new File(testPipeline.getOptions().output))
  }

  @Unroll('#description')
  def 'validate event'() {
    setup:
    def output = testPipeline
        .apply(Create.of(event))
        .apply(ParDo.of(new LeadsPipeline.ValidateEvent())
            .withOutputTags(SUCCESS_TAG, TupleTagList.of(ERROR_TAG)))

    expect:
    PAssert.that(output.get(ERROR_TAG)).containsInAnyOrder(event)
    PAssert.that(output.get(SUCCESS_TAG)).empty()
    testPipeline.run().waitUntilFinish()

    where:
    description                      | event
    'create event without timestamp' | '{"type":"CREATE","data":{"nome":"João RANGEL ","cargo":"Analista de planejamento financeiro"}}'
    'create event without type'      | '{"timestamp":"2020-03-28T19:20:51.638Z","data":{"nome":"João RANGEL ","cargo":"Analista de planejamento financeiro"}}'
    'create event with invalid type' | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"CREATING","data":{"nome":"João RANGEL ","cargo":"Analista de planejamento financeiro"}}'
    'create event without data'      | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"CREATE"}}'
    'create event without nome'      | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"CREATE","data":{"cargo":"Analista de planejamento financeiro"}}'
    'create event without cargo'     | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"CREATE","data":{"nome":"João RANGEL "}}'
    'update event without timestamp' | '{"type":"UPDATE","data":{"id":1098073,"nome":"João PAIVA","cargo":"Auxiliar de marceneiro"}}'
    'update event without type'      | '{"timestamp":"2020-03-28T19:20:51.638Z","data":{"id":1098073,"nome":"João PAIVA","cargo":"Auxiliar de marceneiro"}}'
    'update event with invalid type' | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"UPDATED","data":{"id":1098073,"nome":"João PAIVA","cargo":"Auxiliar de marceneiro"}}'
    'update event without data'      | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"UPDATE"}'
    'update event without id'        | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"UPDATE","data":{"nome":"João PAIVA","cargo":"Auxiliar de marceneiro"}}'
    'update event without nome'      | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"UPDATE","data":{"id":1098073,"cargo":"Auxiliar de marceneiro"}}'
    'update event without cargo'     | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"UPDATE","data":{"id":1098073,"nome":"João PAIVA"}}'
    'delete event without timestamp' | '{"type":"DELETE","data":{"id":1608496}}'
    'delete event without type'      | '{"timestamp":"2020-03-28T19:20:51.638Z","data":{"id":1608496}}'
    'delete event with invalid type' | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"DELETED","data":{"id":1608496}}'
    'delete event without data'      | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"DELETE"}'
    'delete event with empty data'   | '{"timestamp":"2020-03-28T19:20:51.638Z","type":"DELETE","data":{}}'
    'not a json'                     | '<xml>not a json<\\xml>'
    'invalid timestamp'              | '{"timestamp":"2020-03-27 18:28:53 UTC","type":"UPDATE","data":{"id":1098073,"nome":"João PAIVA","cargo":"Auxiliar de marceneiro"}}'
  }

  def createOptions() {
    def pipelineOptions = TestPipeline.testingPipelineOptions().as(LeadsPipelineOptions.class)
    pipelineOptions.output = "/tmp/${UUID.randomUUID().toString()}"
    return pipelineOptions
  }

}
