package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetNodeFeatureParam extends GetParam {
	/**
	 * Node ids the need get feature
	 */
	private long[] nodeIds;

	/**
	 * Feature ids
	 */
	private int[] fids;

	private transient int[] offsets;

	public GetNodeFeatureParam(int matrixId, long[] nodeIds, int[] fids) {
		super(matrixId);
		this.nodeIds = nodeIds;
		this.fids = fids;
	}


	public int[] getOffsets() {
		return offsets;
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public int[] getFids() {
		return fids;
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
				partParams.add(new PartGetNodeFeatureParam(matrixId,
								partitions.get(partIndex), nodeIds, fids, nodeIndex - length, nodeIndex));
			}
			offsets[partIndex] = nodeIndex;
			partIndex++;
		}

		return partParams;
	}
}
