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
  // Replaced wholesale on every update so readers always see a complete, immutable snapshot.
  private volatile Map<String, Object> state = Collections.emptyMap();
  private volatile Object zone;
  private volatile boolean coordinator;
  // Incremented by the owning member on every state change; lets receivers discard stale copies.
  private volatile long version;

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
    return state;
  }

  public void setState(Map<String, Object> state) throws ClusterException {
    this.state =
        state == null || state.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(state));
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

  /** The owner's state version: higher is newer within one incarnation of the member. */
  long getVersion() {
    return version;
  }

  void setVersion(long version) {
    this.version = version;
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
    out.writeObject(new HashMap<>(state));
    out.writeObject(zone);
    out.writeBoolean(coordinator);
    out.writeLong(version);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    name = in.readUTF();
    destination = (JGroupsDestination) in.readObject();
    Map<String, Object> read = (Map<String, Object>) in.readObject();
    state =
        read == null || read.isEmpty()
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(read));
    zone = in.readObject();
    coordinator = in.readBoolean();
    version = in.readLong();
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
