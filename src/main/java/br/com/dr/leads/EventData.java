package br.com.dr.leads;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

import lombok.Value;

@Value
public class EventData implements Serializable {

  private Long id;
  @SerializedName("nome")
  private String name;
  @SerializedName("cargo")
  private String role;

}
