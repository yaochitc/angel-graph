package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetEdgeFeatureParam extends GetParam {
	/**
	 * Edge ids the need get feature
	 */
	private EdgeId[] edgeIds;

	/**
	 * Feature ids
	 */
	private int[] fids;

	private transient int[] offsets;

	public GetEdgeFeatureParam(int matrixId, EdgeId[] edgeIds, int[] fids) {
		super(matrixId);
		this.edgeIds = edgeIds;
		this.fids = fids;
	}

	public int[] getOffsets() {
		return offsets;
	}

	public EdgeId[] getEdgeIds() {
		return edgeIds;
	}

	public int[] getFids() {
		return fids;
	}

	@Override
	public List<PartitionGetParam> split() {
		// Sort the node
		Arrays.sort(edgeIds);

		List<PartitionGetParam> partParams = new ArrayList<>();
		List<PartitionKey> partitions =
						PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

		offsets = new int[partitions.size()];

		int nodeIndex = 0;
		int partIndex = 0;
		while (nodeIndex < edgeIds.length || partIndex < partitions.size()) {
			int length = 0;
			int endOffset = (int) partitions.get(partIndex).getEndCol();
			while (nodeIndex < edgeIds.length && edgeIds[nodeIndex].getFromNodeId() < endOffset) {
				nodeIndex++;
				length++;
			}

			if (length > 0) {
				partParams.add(new PartGetEdgeFeatureParam(matrixId,
								partitions.get(partIndex), edgeIds, fids, nodeIndex - length, nodeIndex));
			}
			offsets[partIndex] = nodeIndex;
			partIndex++;
		}

		return partParams;
	}
}
