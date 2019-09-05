package com.tencent.angel.graph.data.graph;

import java.util.Objects;

public class EdgeId {
  private long fromNodeId;
  private long toNodeId;
  private int type;

  public EdgeId(long fromNodeId, long toNodeId, int type) {
    this.fromNodeId = fromNodeId;
    this.toNodeId = toNodeId;
    this.type = type;
  }

  public long getFromNodeId() {
    return fromNodeId;
  }

  public long getToNodeId() {
    return toNodeId;
  }

  public int getType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    EdgeId edgeId = (EdgeId) o;
    return fromNodeId == edgeId.fromNodeId &&
            toNodeId == edgeId.toNodeId &&
            type == edgeId.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromNodeId, toNodeId, type);
  }
}
