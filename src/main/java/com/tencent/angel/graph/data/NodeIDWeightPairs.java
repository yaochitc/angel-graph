package com.tencent.angel.graph.data;

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

public class NodeIDWeightPairs implements Serialize {

  /**
   * Edge types
   */
  int[] edgeTypes;

  /**
   * Neighbor node ids
   */
  long[] nodeIds;

  /**
   * Neighbor node weights
   */
  float[] nodeWeights;

  public NodeIDWeightPairs(int[] edgeTypes, long[] nodeIds, float[] nodeWeights) {
    this.edgeTypes = edgeTypes;
    this.nodeIds = nodeIds;
    this.nodeWeights = nodeWeights;
  }

  public NodeIDWeightPairs() {
    this(null, null, null);
  }


  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(int[] edgeTypes) {
    this.edgeTypes = edgeTypes;
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public float[] getNodeWeights() {
    return nodeWeights;
  }

  public void setNodeWeights(float[] nodeWeights) {
    this.nodeWeights = nodeWeights;
  }

  public int size() {
    return nodeIds.length;
  }

  public static NodeIDWeightPairs empty() {
  	return new NodeIDWeightPairs(new int[0], new long[0], new float[0]);
	}

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(edgeTypes.length);
    for (int i = 0; i < edgeTypes.length; i++) {
      output.writeInt(edgeTypes[i]);
    }

    output.writeInt(nodeIds.length);
    for (int i = 0; i < nodeIds.length; i++) {
      output.writeLong(nodeIds[i]);
    }

    output.writeInt(nodeWeights.length);
    for (int i = 0; i < nodeWeights.length; i++) {
      output.writeFloat(nodeWeights[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    edgeTypes = new int[input.readInt()];
    for (int i = 0; i < edgeTypes.length; i++) {
      edgeTypes[i] = input.readInt();
    }

    nodeIds = new long[input.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = input.readLong();
    }

    nodeWeights = new float[input.readInt()];
    for (int i = 0; i < nodeWeights.length; i++) {
      nodeWeights[i] = input.readFloat();
    }
  }

  @Override
  public int bufferLen() {
    return (4 + 4 * edgeTypes.length) + (4 + 8 * nodeIds.length) + (4
        + 4 * nodeWeights.length);
  }
}
