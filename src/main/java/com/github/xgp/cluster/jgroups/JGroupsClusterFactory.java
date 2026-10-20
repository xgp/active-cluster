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
 * Creates {@link JGroupsCluster} members that all use the same JGroups stack. The stack is an XML
 * file path or classpath resource; with none given, {@code minimal.xml} (UDP multicast) is used.
 * Clusters are created unstarted: add listeners, then call {@link Cluster#start()}.
 */
public class JGroupsClusterFactory implements ClusterFactory {

  private final String jgroupsConfig;
  private final JGroupsDestinationMarshaller marshaller;

  /** Create a new factory with the default stack, {@code minimal.xml}. */
  public JGroupsClusterFactory() {
    this(null, null);
  }

  /**
   * Create a new factory with the given JGroups configuration.
   *
   * @param jgroupsConfig JGroups XML file path or classpath resource, or null for the default
   */
  public JGroupsClusterFactory(String jgroupsConfig) {
    this(jgroupsConfig, null);
  }

  /**
   * Create a new factory with the given JGroups configuration and marshaller.
   *
   * @param jgroupsConfig JGroups XML file path or classpath resource, or null for the default
   * @param marshaller destination marshaller, or null for the default
   */
  public JGroupsClusterFactory(String jgroupsConfig, JGroupsDestinationMarshaller marshaller) {
    this.jgroupsConfig = jgroupsConfig;
    this.marshaller = marshaller != null ? marshaller : new JGroupsDestinationMarshaller();
  }

  /**
   * @throws ClusterException if a name is null or empty, or the node name equals the cluster name
   */
  @Override
  public Cluster createCluster(String localName, String destinationName) throws ClusterException {
    try {
      return new JGroupsCluster(localName, destinationName, jgroupsConfig);
    } catch (IllegalArgumentException e) {
      throw new ClusterException(e.getMessage(), e);
    }
  }

  @Override
  public Cluster createCluster(
      String localName, String destinationName, DestinationMarshaller marshaller)
      throws ClusterException {
    requireJGroups(marshaller);
    return createCluster(localName, destinationName);
  }

  /** Creates a member named {@code node-<random UUID>}. */
  @Override
  public Cluster createCluster(String destinationName) throws ClusterException {
    return createCluster("node-" + UUID.randomUUID(), destinationName);
  }

  @Override
  public Cluster createCluster(String localName, Destination destination) throws ClusterException {
    return createCluster(localName, nameOf(destination, marshaller));
  }

  @Override
  public Cluster createCluster(
      String localName, Destination destination, DestinationMarshaller marshaller)
      throws ClusterException {
    requireJGroups(marshaller);
    return createCluster(localName, nameOf(destination, marshaller));
  }

  @Override
  public Cluster createCluster(Destination destination) throws ClusterException {
    return createCluster(nameOf(destination, marshaller));
  }

  private static void requireJGroups(DestinationMarshaller marshaller) throws ClusterException {
    if (!(marshaller instanceof JGroupsDestinationMarshaller)) {
      throw new ClusterException("Not a JGroups destination marshaller: " + marshaller);
    }
  }

  private static String nameOf(Destination destination, DestinationMarshaller marshaller)
      throws ClusterException {
    String name = marshaller.getDestinationName(destination);
    if (name == null) {
      throw new ClusterException("Not a JGroups destination: " + destination);
    }
    return name;
  }
}
