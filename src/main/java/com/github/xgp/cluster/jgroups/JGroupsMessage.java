package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.Message;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * JGroups implementation of the Message interface.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsMessage implements Message, Serializable {

  private static final long serialVersionUID = 7398930541981396056L;

  private final String id;
  private final Map<String, Object> properties = new HashMap<>();
  private Object body;

  public JGroupsMessage() {
    this.id = UUID.randomUUID().toString();
  }

  public JGroupsMessage(Object body) {
    this();
    this.body = body;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void acknowledge() {
    // No-op in JGroups implementation
  }

  @Override
  public void clearProperties() {
    properties.clear();
  }

  @Override
  public Map<String, Object> getProperties() {
    return properties;
  }

  @Override
  public void setProperty(String key, Object value) {
    properties.put(key, value);
  }

  @Override
  public Object getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public void clearBody() {
    body = null;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getBody(Class<T> c) {
    if (body == null) {
      return null;
    }

    if (c.isInstance(body)) {
      return (T) body;
    }

    return null;
  }

  public void setBody(Object body) {
    this.body = body;
  }

  @Override
  public String toString() {
    return "JGroupsMessage[id=" + id + "]";
  }
}
