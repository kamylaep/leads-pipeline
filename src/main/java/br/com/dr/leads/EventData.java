package br.com.dr.leads;

import java.io.Serializable;

import lombok.Value;

@Value
public class EventData implements Serializable {

  private Long id;
  private String nome;
  private String cargo;

}
