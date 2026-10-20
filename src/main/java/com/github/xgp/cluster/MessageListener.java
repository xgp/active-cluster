package com.github.xgp.cluster;

/** Receives messages pushed by a {@link MessageConsumer}. */
public interface MessageListener {

  /**
   * Called for each message delivered to the consumer, on an implementation thread. It should
   * return quickly; an exception is logged and does not affect other consumers.
   *
   * @param message the message
   */
  void onMessage(Message message);
}
