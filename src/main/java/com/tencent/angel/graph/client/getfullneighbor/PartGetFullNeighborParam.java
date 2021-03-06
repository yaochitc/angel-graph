package com.tencent.angel.graph.client.getfullneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Parameters for part get full neighbors
 */
public class PartGetFullNeighborParam extends PartitionGetParam {

  /**
   * Node ids: it just a view for original node ids
   */
  private long[] nodeIds;

  /**
   * Edge types
   */
  private int[] edgeTypes;

  /**
   * Store position: start index in nodeIds
   */
  private int startIndex;

  /**
   * Store position: end index in nodeIds
   */
  private int endIndex;

  public PartGetFullNeighborParam(int matrixId, PartitionKey part, long[] nodeIds, int[] edgeTypes
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.edgeTypes = edgeTypes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartGetFullNeighborParam() {
    this(0, null, null, null, 0, 0);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(endIndex - startIndex);

    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(nodeIds[i]);
    }

    if (edgeTypes == null) {
      buf.writeInt(0);
    } else {
      buf.writeInt(edgeTypes.length);
      for (int i = 0; i < edgeTypes.length; i++) {
        buf.writeInt(edgeTypes[i]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIds = new long[len];
    for (int i = 0; i < len; i++) {
      nodeIds[i] = buf.readLong();
    }

    len = buf.readInt();
    edgeTypes = new int[len];
    for (int i = 0; i < len; i++) {
      edgeTypes[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 8 * nodeIds.length + 4 + ((edgeTypes == null) ? 0
        : 4 * edgeTypes.length);
  }
}
