package br.com.dr.leads;

import java.io.Serializable;

import com.google.gson.annotations.SerializedName;

import lombok.Value;

@Value(staticConstructor = "create")
public class EventData implements Serializable {

  private Long id;
  @SerializedName("nome")
  private String name;
  @SerializedName("cargo")
  private String jobTitle;
  @SerializedName("salarioMedio")
  private Double averageSalary;

}
