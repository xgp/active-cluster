package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.Node;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.jgroups.Address;

/**
 * JGroups implementation of the Node interface.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsNode implements Node, Externalizable {

  private static final long serialVersionUID = -3697243211359033124L;

  private String name;
  private JGroupsDestination destination;
  private Map<String, Object> state = new HashMap<>();
  private Object zone;
  private boolean coordinator;

  // Required for Externalizable
  public JGroupsNode() {}

  public JGroupsNode(String name) {
    this.name = name;
    this.destination = new JGroupsDestination(name);
  }

  public JGroupsNode(String name, Address address) {
    this.name = name;
    this.destination = new JGroupsDestination(name, address);
  }

  @Override
  public Destination getDestination() {
    return destination;
  }

  @Override
  public Map<String, Object> getState() {
    return Collections.unmodifiableMap(state);
  }

  public void setState(Map<String, Object> state) throws ClusterException {
    this.state.clear();
    if (state != null) {
      this.state.putAll(state);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public boolean isCoordinator() {
    return coordinator;
  }

  public void setCoordinator(boolean coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public Object getZone() {
    return zone;
  }

  public void setZone(Object zone) {
    this.zone = zone;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeUTF(name);
    out.writeObject(destination);
    out.writeObject(state);
    out.writeObject(zone);
    out.writeBoolean(coordinator);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    name = in.readUTF();
    destination = (JGroupsDestination) in.readObject();
    state = (Map<String, Object>) in.readObject();
    zone = in.readObject();
    coordinator = in.readBoolean();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JGroupsNode that = (JGroupsNode) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "JGroupsNode[name=" + name + ", coordinator=" + coordinator + "]";
  }
}
