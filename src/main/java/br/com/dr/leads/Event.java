package br.com.dr.leads;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import lombok.Value;

@Value
public class Event implements Serializable {

  private String timestamp;
  private String type;
  private EventData data;

  public boolean isValid() {
    if (!isValidTimestamp() || !EventType.isValid(type) || Objects.isNull(data)) {
      return false;
    }

    EventType eventType = EventType.valueOf(getType());

    if (EventType.CREATE == eventType) {
      return !StringUtils.isAnyBlank(data.getName(), data.getRole());
    }

    if (EventType.UPDATE == eventType) {
      return Objects.nonNull(data.getId()) && !StringUtils.isAnyBlank(data.getName(), data.getRole());
    }

    if (EventType.DELETE == eventType) {
      return Objects.nonNull(data.getId());
    }

    return false;
  }

  private boolean isValidTimestamp() {
    try {
      LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

}
