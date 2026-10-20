package com.github.xgp.cluster.jgroups;

import java.io.Serializable;

/**
 * Internal protocol message exchanged between {@link JGroupsCluster} members. A member that stops
 * cleanly multicasts {@link Kind#LEAVING} and waits for a {@link Kind#LEAVE_ACK} from every other
 * member, so that survivors can tell a clean leave ({@code REMOVE_NODE}) from a crash ({@code
 * FAILED_NODE} then {@code REMOVE_NODE}) when the next view drops the member.
 */
final class ControlMessage implements Serializable {

  private static final long serialVersionUID = 1L;

  enum Kind {
    LEAVING,
    LEAVE_ACK
  }

  private final Kind kind;
  private final String nodeName;

  ControlMessage(Kind kind, String nodeName) {
    this.kind = kind;
    this.nodeName = nodeName;
  }

  Kind getKind() {
    return kind;
  }

  String getNodeName() {
    return nodeName;
  }

  @Override
  public String toString() {
    return "ControlMessage[" + kind + " " + nodeName + "]";
  }
}
