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

import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.LocalNode;
import java.util.HashMap;
import java.util.Map;
import org.jgroups.Address;
import org.jgroups.JChannel;

/**
 * JGroups implementation of the LocalNode interface.
 * 
 * @version $Revision: 1.0 $
 */
public class JGroupsLocalNode extends JGroupsNode implements LocalNode {
  
  private static final long serialVersionUID = -7213743750639969514L;
  
  private transient JGroupsCluster cluster;
  
  /**
   * No-arg constructor required for serialization
   */
  public JGroupsLocalNode() {
    super();
  }
  
  public JGroupsLocalNode(String name) {
    super(name);
  }
  
  public JGroupsLocalNode(String name, Address address) {
    super(name, address);
  }
  
  public void setCluster(JGroupsCluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public void setState(Map<String, Object> state) throws ClusterException {
    if (cluster == null) {
      throw new ClusterException();
    }
    
    Map<String, Object> copy = new HashMap<>();
    if (state != null) {
      copy.putAll(state);
    }
    
    super.setState(copy);
    
    try {
      cluster.broadcastNodeState();
    } catch (Exception e) {
      throw new ClusterException();
    }
  }
  
  /**
   * Get the JChannel associated with this local node through its cluster.
   * 
   * @return the JGroups channel or null if not connected to a cluster
   */
  public JChannel getChannel() {
    return cluster != null ? cluster.getChannel() : null;
  }
}