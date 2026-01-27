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
package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.ClusterFactory;
import com.github.xgp.cluster.Destination;
import com.github.xgp.cluster.DestinationMarshaller;
import java.util.UUID;

/**
 * JGroups implementation of the ClusterFactory interface.
 *
 * @version $Revision: 1.0 $
 */
public class JGroupsClusterFactory implements ClusterFactory {

  private String jgroupsConfig;
  private JGroupsDestinationMarshaller marshaller;

  /** Create a new factory with default configuration. */
  public JGroupsClusterFactory() {
    this(null, null);
  }

  /**
   * Create a new factory with the given JGroups configuration file.
   *
   * @param jgroupsConfig path to JGroups XML configuration file
   */
  public JGroupsClusterFactory(String jgroupsConfig) {
    this(jgroupsConfig, null);
  }

  /**
   * Create a new factory with the given JGroups configuration and marshaller.
   *
   * @param jgroupsConfig path to JGroups XML configuration file
   * @param marshaller custom destination marshaller or null for default
   */
  public JGroupsClusterFactory(String jgroupsConfig, JGroupsDestinationMarshaller marshaller) {
    this.jgroupsConfig = jgroupsConfig;
    this.marshaller = marshaller != null ? marshaller : new JGroupsDestinationMarshaller();
  }

  @Override
  public Cluster createCluster(String localName, String destinationName) throws ClusterException {
    JGroupsCluster cluster =
        new JGroupsCluster(localName, destinationName, jgroupsConfig, marshaller);
    return cluster;
  }

  @Override
  public Cluster createCluster(
      String localName, String destinationName, DestinationMarshaller marshaller)
      throws ClusterException {
    if (!(marshaller instanceof JGroupsDestinationMarshaller)) {
      throw new ClusterException();
    }

    JGroupsCluster cluster =
        new JGroupsCluster(
            localName, destinationName, jgroupsConfig, (JGroupsDestinationMarshaller) marshaller);

    return cluster;
  }

  @Override
  public Cluster createCluster(String destinationName) throws ClusterException {
    // Generate a unique local node name
    String localName = "node-" + UUID.randomUUID().toString();
    return createCluster(localName, destinationName);
  }

  @Override
  public Cluster createCluster(String localName, Destination destination) throws ClusterException {
    if (!(destination instanceof JGroupsDestination)) {
      throw new ClusterException();
    }

    String destName = marshaller.getDestinationName(destination);
    if (destName == null) {
      throw new ClusterException();
    }

    return createCluster(localName, destName);
  }

  @Override
  public Cluster createCluster(
      String localName, Destination destination, DestinationMarshaller marshaller)
      throws ClusterException {

    if (!(destination instanceof JGroupsDestination)
        || !(marshaller instanceof JGroupsDestinationMarshaller)) {
      throw new ClusterException();
    }

    String destName = marshaller.getDestinationName(destination);
    if (destName == null) {
      throw new ClusterException();
    }

    return createCluster(localName, destName, marshaller);
  }

  @Override
  public Cluster createCluster(Destination destination) throws ClusterException {
    if (!(destination instanceof JGroupsDestination)) {
      throw new ClusterException();
    }

    String destName = marshaller.getDestinationName(destination);
    if (destName == null) {
      throw new ClusterException();
    }

    return createCluster(destName);
  }
}
