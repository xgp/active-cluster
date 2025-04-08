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
package com.github.xgp.cluster.examples;

import com.github.xgp.cluster.Cluster;
import com.github.xgp.cluster.ClusterEvent;
import com.github.xgp.cluster.ClusterException;
import com.github.xgp.cluster.ClusterListener;
import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.MessageListener;
import com.github.xgp.cluster.Node;
import com.github.xgp.cluster.impl.jgroups.JGroupsClusterFactory;
import com.github.xgp.cluster.impl.jgroups.JGroupsMessage;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * Example application demonstrating the use of JGroups-based cluster implementation.
 * 
 * <p>Run multiple instances of this application to create a cluster.
 */
public class JGroupsClusterExample {

  public static void main(String[] args) {
    try {
      // Create a unique node name or allow user to specify one
      String nodeName = "node-" + System.currentTimeMillis();
      String configFile = null;
      
      if (args.length > 0) {
        nodeName = args[0];
      }
      
      if (args.length > 1) {
        configFile = args[1];
      }
      
      System.out.println("Starting cluster node: " + nodeName);
      
      // Create the cluster factory
      JGroupsClusterFactory factory = new JGroupsClusterFactory(configFile);
      
      // Create and start the cluster
      Cluster cluster = factory.createCluster(nodeName, "example-cluster");
      cluster.start();
      
      // Add a listener to monitor cluster events
      cluster.addClusterListener(new SimpleClusterListener());
      
      // Create a message consumer for the cluster destination with some extra logging
      MessageConsumer consumer = cluster.createConsumer(cluster.getDestination());
      consumer.setMessageListener(new SimpleMessageListener());
      
      System.out.println("Message consumer registered for cluster destination");
      System.out.println("Local node address: " + cluster.getLocalNode().getDestination());
      
      // Update local node state
      Map<String, Object> state = new HashMap<>();
      state.put("startTime", System.currentTimeMillis());
      state.put("hostname", System.getProperty("user.name"));
      cluster.getLocalNode().setState(state);
      
      // Command-line interface
      Scanner scanner = new Scanner(System.in);
      boolean running = true;
      
      System.out.println("Cluster node started. Commands: send <message>, nodes, quit");
      
      while (running) {
        System.out.print("> ");
        String line = scanner.nextLine().trim();
        
        if (line.startsWith("send ")) {
          String msgText = line.substring(5);
          Message msg = new JGroupsMessage(msgText);
          msg.setProperty("sender", nodeName);
          msg.setProperty("timestamp", System.currentTimeMillis());
          
          cluster.send(cluster.getDestination(), msg);
          System.out.println("Message sent to cluster");
        } else if (line.equals("nodes")) {
          System.out.println("Current cluster nodes:");
          for (Object value : cluster.getNodes().values()) {
            Node node = (Node) value;
            String coordStatus = node.isCoordinator() ? " (coordinator)" : "";
            System.out.println("  - " + node.getName() + coordStatus);
            System.out.println("    State: " + node.getState());
          }
        } else if (line.equals("quit") || line.equals("exit")) {
          running = false;
        } else {
          System.out.println("Unknown command. Available: send <message>, nodes, quit");
        }
      }
      
      // Shutdown
      System.out.println("Shutting down cluster node...");
      cluster.stop();
      scanner.close();
      
    } catch (Exception e) {
      System.err.println("Error: " + e.getMessage());
      e.printStackTrace();
    }
  }
  
  /**
   * Simple implementation of ClusterListener to display cluster events.
   */
  static class SimpleClusterListener implements ClusterListener {
    
    @Override
    public void onNodeAdd(ClusterEvent event) {
      System.out.println("[CLUSTER] Node added: " + event.getNode().getName());
    }

    @Override
    public void onNodeUpdate(ClusterEvent event) {
      System.out.println("[CLUSTER] Node updated: " + event.getNode().getName());
    }

    @Override
    public void onNodeRemoved(ClusterEvent event) {
      System.out.println("[CLUSTER] Node removed: " + event.getNode().getName());
    }

    @Override
    public void onNodeFailed(ClusterEvent event) {
      System.out.println("[CLUSTER] Node failed: " + event.getNode().getName());
    }

    @Override
    public void onCoordinatorChanged(ClusterEvent event) {
      System.out.println("[CLUSTER] New coordinator: " + event.getNode().getName());
    }
  }
  
  /**
   * Simple implementation of MessageListener to display received messages.
   */
  static class SimpleMessageListener implements MessageListener {
    
    @Override
    public void onMessage(Message message) {
      String sender = (String) message.getProperty("sender");
      String text = message.getBody(String.class);
      System.out.println("\n[MESSAGE from " + sender + "] " + text);
      System.out.print("> "); // Reprint the prompt
    }
  }
}