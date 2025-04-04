package com.github.xgp.cluster;

import java.util.Map;

public interface Message {
  String getId();

  void acknowledge();

  void clearProperties();

  Map<String, Object> getProperties();

  void setProperty(String key, Object value);

  Object getProperty(String key);

  void clearBody();

  <T> T getBody(Class<T> c);
}
