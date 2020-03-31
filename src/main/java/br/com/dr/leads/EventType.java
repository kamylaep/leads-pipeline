package br.com.dr.leads;

import java.io.Serializable;
import java.util.Arrays;

public enum EventType implements Serializable {

  CREATE,
  UPDATE,
  DELETE;

  public static boolean isValid(String type) {
    return Arrays.stream(values()).anyMatch(t -> EventType.valueOf(type) == t);
  }

}
