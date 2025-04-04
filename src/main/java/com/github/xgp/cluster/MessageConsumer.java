package com.github.xgp.cluster;

public interface MessageConsumer extends AutoCloseable {

  MessageListener getMessageListener() throws Exception;

  void setMessageListener(MessageListener listener) throws Exception;

  Message receive() throws Exception;

  Message receive(long timeout) throws Exception;

  Message receiveNoWait() throws Exception;

  @Override
  void close() throws Exception;
}
