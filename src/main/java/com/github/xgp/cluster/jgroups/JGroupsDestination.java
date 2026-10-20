package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.Destination;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import org.jgroups.Address;
import org.jgroups.util.Util;

/**
 * JGroups implementation of the Destination interface.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsDestination implements Destination, Serializable {

  private static final long serialVersionUID = -5390829685978935783L;

  private final String name;
  private transient volatile Address address;

  public JGroupsDestination(String name) {
    this(name, null);
  }

  public JGroupsDestination(String name, Address address) {
    this.name = java.util.Objects.requireNonNull(name, "name");
    this.address = address;
  }

  public String getName() {
    return name;
  }

  public Address getAddress() {
    return address;
  }

  public void setAddress(Address address) {
    this.address = address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    JGroupsDestination that = (JGroupsDestination) o;
    return name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }

  @Override
  public String toString() {
    return "JGroupsDestination[" + name + "]";
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    Util.writeAddress(address, out);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    address = Util.readAddress(in);
  }
}
