package com.tencent.angel.graph.client.getfullneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Parameters for GetNeighbor
 */
public class GetFullNeighborParam extends GetParam {
  /**
   * Node ids the need get neighbors
   */
  private long[] nodeIds;

  /**
   * Edge types
   */
  private int[] edgeTypes;

  /**
   * Just store the split positions
   */
  private transient int [] offsets;

  public GetFullNeighborParam(int matrixId, long[] nodeIds, int[] edgeTypes) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.edgeTypes = edgeTypes;
  }

  public int[] getOffsets() {
    return offsets;
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Sort the node
    Arrays.sort(nodeIds);

    List<PartitionGetParam> partParams = new ArrayList<>();
    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    offsets = new int[partitions.size()];

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < nodeIds.length || partIndex < partitions.size()) {
      int length = 0;
      int endOffset = (int) partitions.get(partIndex).getEndCol();
      while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        partParams.add(new PartGetFullNeighborParam(matrixId,
            partitions.get(partIndex), nodeIds, edgeTypes, nodeIndex - length, nodeIndex));
      }
      offsets[partIndex] = nodeIndex;
      partIndex++;
    }

    return partParams;
  }
}
