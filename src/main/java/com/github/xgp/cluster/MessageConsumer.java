package com.github.xgp.cluster;

/**
 * Receives the messages sent to one destination. Messages are pushed to the {@link MessageListener}
 * if one is set, and otherwise queued for the {@code receive} methods.
 */
public interface MessageConsumer extends AutoCloseable {

  /**
   * @return the listener messages are pushed to, or null if they are queued
   * @throws Exception if the consumer cannot report its listener
   */
  MessageListener getMessageListener() throws Exception;

  /**
   * Sets the listener that incoming messages are pushed to. Messages already queued stay queued.
   *
   * @param listener the listener, or null to queue messages for {@link #receive}
   * @throws Exception if the listener cannot be set
   */
  void setMessageListener(MessageListener listener) throws Exception;

  /**
   * Waits for the next queued message.
   *
   * @return the next message
   * @throws Exception if the consumer is closed or the wait is interrupted
   */
  Message receive() throws Exception;

  /**
   * Waits up to {@code timeout} milliseconds for the next queued message.
   *
   * @param timeout the maximum wait in milliseconds
   * @return the next message, or null if none arrived in time
   * @throws Exception if the consumer is closed or the wait is interrupted
   */
  Message receive(long timeout) throws Exception;

  /**
   * @return the next queued message, or null if there is none
   * @throws Exception if the consumer is closed
   */
  Message receiveNoWait() throws Exception;

  /**
   * Stops delivery to this consumer. Stopping the cluster closes all of its consumers.
   *
   * @throws Exception if closing fails
   */
  @Override
  void close() throws Exception;
}
