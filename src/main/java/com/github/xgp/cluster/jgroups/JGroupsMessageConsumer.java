package com.github.xgp.cluster.jgroups;

import com.github.xgp.cluster.Message;
import com.github.xgp.cluster.MessageConsumer;
import com.github.xgp.cluster.MessageListener;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Receives messages for one destination: the cluster destination (broadcasts) or the local node's
 * destination (direct messages). Messages are handed to the {@link MessageListener} if one is set,
 * otherwise queued for {@link #receive}.
 *
 * <p>The optional selector is a single property equality, {@code key='value'} or {@code key=value};
 * it matches messages whose property {@code key} has the string form {@code value}.
 */
public class JGroupsMessageConsumer implements MessageConsumer {

  private static final Logger LOG = Logger.getLogger(JGroupsMessageConsumer.class.getName());

  private final JGroupsCluster cluster;
  private final JGroupsDestination destination;
  private final String selector;
  private final String selectorKey;
  private final String selectorValue;
  private final boolean noLocal;
  private final BlockingQueue<Message> messageQueue = new LinkedBlockingQueue<>();

  private volatile MessageListener messageListener;
  private volatile boolean closed = false;

  /**
   * @throws IllegalArgumentException if the selector is not of the form {@code key=value}
   */
  public JGroupsMessageConsumer(
      JGroupsCluster cluster, JGroupsDestination destination, String selector, boolean noLocal) {
    String[] parsed = validateSelector(selector);
    this.cluster = cluster;
    this.destination = destination;
    this.selector = selector;
    this.selectorKey = parsed == null ? null : parsed[0];
    this.selectorValue = parsed == null ? null : parsed[1];
    this.noLocal = noLocal;

    cluster.registerConsumer(this);
  }

  /**
   * Parses a selector into {@code [key, value]}, or returns null for a null selector.
   *
   * @throws IllegalArgumentException if the selector is not of the form {@code key=value}
   */
  static String[] validateSelector(String selector) {
    if (selector == null) {
      return null;
    }
    String[] parts = selector.split("=", -1);
    if (parts.length != 2) {
      throw new IllegalArgumentException(
          "Unsupported selector '" + selector + "': expected key='value'");
    }
    String key = parts[0].trim();
    String value = parts[1].trim();
    if (value.length() >= 2 && value.startsWith("'") && value.endsWith("'")) {
      value = value.substring(1, value.length() - 1);
    }
    if (key.isEmpty() || value.isEmpty()) {
      throw new IllegalArgumentException(
          "Unsupported selector '" + selector + "': expected key='value'");
    }
    return new String[] {key, value};
  }

  @Override
  public MessageListener getMessageListener() {
    return messageListener;
  }

  @Override
  public void setMessageListener(MessageListener listener) {
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
  public void close() {
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
   * Delivers a message to the listener, or queues it. A listener that throws is logged and does not
   * affect delivery to other consumers.
   */
  public void onMessage(Message message) {
    if (closed) {
      return;
    }

    MessageListener listener = messageListener;
    if (listener != null) {
      try {
        listener.onMessage(message);
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Message listener on " + destination + " failed", e);
      }
    } else {
      messageQueue.offer(message);
    }
  }

  private void checkClosed() throws IllegalStateException {
    if (closed) {
      throw new IllegalStateException("Consumer is closed");
    }
  }

  public boolean matchesSelector(Message message) {
    if (selectorKey == null) {
      return true;
    }
    Object propValue = message.getProperty(selectorKey);
    return propValue != null && propValue.toString().equals(selectorValue);
  }
}
