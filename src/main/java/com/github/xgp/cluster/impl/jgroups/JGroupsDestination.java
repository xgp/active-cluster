/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.xgp.cluster.impl.jgroups;

import com.github.xgp.cluster.Destination;
import java.io.Serializable;
import org.jgroups.Address;

/**
 * JGroups implementation of the Destination interface.
 * 
 * @version $Revision: 1.0 $
 */
public class JGroupsDestination implements Destination, Serializable {
  
  private static final long serialVersionUID = -5390829685978935783L;
  
  private final String name;
  private transient Address address;
  
  public JGroupsDestination(String name) {
    this.name = name;
  }
  
  public JGroupsDestination(String name, Address address) {
    this.name = name;
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
}