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
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.DestinationMarshaller;

/**
 * JGroups implementation of the DestinationMarshaller interface.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsDestinationMarshaller implements DestinationMarshaller {

  /**
   * Builds a destination from a destinationName
   *
   * @param destinationName
   * @return the destination to send messages to all members of the cluster
   */
  @Override
  public Destination getDestination(String destinationName) throws ClusterException {
    if (destinationName == null || destinationName.isEmpty()) {
      throw new ClusterException();
    }
    return new JGroupsDestination(destinationName);
  }

  /**
   * Gets a destination's physical name
   *
   * @param destination
   * @return the destination's physical name
   */
  @Override
  public String getDestinationName(Destination destination) {
    if (destination instanceof JGroupsDestination) {
      return ((JGroupsDestination) destination).getName();
    }
    return null;
  }
}