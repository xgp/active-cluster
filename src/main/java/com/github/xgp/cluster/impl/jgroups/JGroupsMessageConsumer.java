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

import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.MessageListener;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * JGroups implementation of the MessageConsumer interface.
 * 
 * @version $Revision: 1.0 $
 */
public class JGroupsMessageConsumer implements MessageConsumer {

  private final JGroupsCluster cluster;
  private final JGroupsDestination destination;
  private final String selector;
  private final boolean noLocal;
  private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();
  
  private MessageListener messageListener;
  private boolean closed = false;
  
  public JGroupsMessageConsumer(
      JGroupsCluster cluster, JGroupsDestination destination, String selector, boolean noLocal) {
    this.cluster = cluster;
    this.destination = destination;
    this.selector = selector;
    this.noLocal = noLocal;
    
    cluster.registerConsumer(this);
  }

  @Override
  public MessageListener getMessageListener() throws Exception {
    return messageListener;
  }

  @Override
  public void setMessageListener(MessageListener listener) throws Exception {
    this.messageListener = listener;
  }

  @Override
  public Message receive() throws Exception {
    checkClosed();
    return messageQueue.take();
  }

  @Override
  public Message receive(long timeout) throws Exception {
    checkClosed();
    return messageQueue.poll(timeout, TimeUnit.MILLISECONDS);
  }

  @Override
  public Message receiveNoWait() throws Exception {
    checkClosed();
    return messageQueue.poll();
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      closed = true;
      cluster.unregisterConsumer(this);
    }
  }
  
  public JGroupsDestination getDestination() {
    return destination;
  }
  
  public String getSelector() {
    return selector;
  }
  
  public boolean isNoLocal() {
    return noLocal;
  }
  
  /**
   * Called when a message is received from JGroups that matches this consumer's criteria.
   * 
   * @param message the received message
   */
  public void onMessage(Message message) {
    if (closed) {
      return;
    }
    
    if (messageListener != null) {
      messageListener.onMessage(message);
    } else {
      messageQueue.offer(message);
    }
  }
  
  private void checkClosed() throws Exception {
    if (closed) {
      throw new Exception("Consumer is closed");
    }
  }
  
  /**
   * Checks if this message matches the selector criteria.
   * 
   * @param message the message to check
   * @return true if the message passes the selector, false otherwise
   */
  public boolean matchesSelector(Message message) {
    // Simple selector implementation - could be enhanced to use SQL92 syntax
    if (selector == null) {
      return true;
    }
    
    // Example: Simple property matching
    String[] parts = selector.split("=");
    if (parts.length == 2) {
      String key = parts[0].trim();
      String value = parts[1].trim().replace("'", "");
      
      Object propValue = message.getProperty(key);
      return propValue != null && propValue.toString().equals(value);
    }
    
    return true;
  }
}