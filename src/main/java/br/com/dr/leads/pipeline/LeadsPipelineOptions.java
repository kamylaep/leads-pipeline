package br.com.dr.leads.pipeline;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;

public interface LeadsPipelineOptions extends GcpOptions {

  String getSubscription();

  void setSubscription(String subscription);

  String getOutput();

  void setOutput(String output);

  @Default.Long(15 * 60)
  long getWindowInSeconds();

  void setWindowInSeconds(long windowInSeconds);

  @Default.Integer(1)
  int getShardsNum();

  void setShardsNum(int shardsNum);

}
