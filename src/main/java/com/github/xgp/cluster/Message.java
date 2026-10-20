package com.github.xgp.cluster;

import java.util.Map;

/**
 * A message: an identifier, a map of properties (which selectors filter on) and a body. Property
 * values and the body must be serializable by the implementation.
 */
public interface Message {

  /**
   * @return an identifier unique to this message
   */
  String getId();

  /** Acknowledges receipt. Implementations without acknowledgement treat this as a no-op. */
  void acknowledge();

  /** Removes all properties. */
  void clearProperties();

  /**
   * @return the properties of this message
   */
  Map<String, Object> getProperties();

  /**
   * Sets a property.
   *
   * @param key the property name
   * @param value the property value
   */
  void setProperty(String key, Object value);

  /**
   * @param key the property name
   * @return the property value, or null if it is not set
   */
  Object getProperty(String key);

  /** Removes the body. */
  void clearBody();

  /**
   * Returns the body if it is an instance of {@code c}.
   *
   * @param <T> the expected body type
   * @param c the expected body class
   * @return the body, or null if there is none or it is not a {@code c}
   */
  <T> T getBody(Class<T> c);
}
