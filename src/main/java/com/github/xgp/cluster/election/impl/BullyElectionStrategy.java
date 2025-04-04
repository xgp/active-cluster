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
package com.github.xgp.cluster.election.impl;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.election.ElectionStrategy;
import java.util.Iterator;
import java.util.Map;

/**
 * <code>BullyElectionStrategy</code> Use a simple bully algorithm to elect a coordinator. the
 * member with the lowest lexicographical name is choosen
 *
 * @version $Revision: 1.2 $
 */
public class BullyElectionStrategy implements ElectionStrategy {

  /**
   * Elect a coordinator.
   *
   * @param cluster
   * @return the elected Node
   * @throws ClusterException
   */
  public Node doElection(Cluster cluster) throws ClusterException {
    Node elect = cluster.getLocalNode();

    Map nodes = cluster.getNodes();
    for (Iterator i = nodes.values().iterator(); i.hasNext(); ) {
      Node node = (Node) i.next();
      if (elect.getName().compareTo(node.getName()) < 0) {
        elect = node;
      }
    }

    return elect;
  }
}
