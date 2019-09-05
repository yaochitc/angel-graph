package com.tencent.angel.graph.client.sampleedge;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SampleEdgeParam extends GetParam {
	private int nodeType;

	private int count;

	private Map<Integer, Integer> countPerPart;

	public SampleEdgeParam(int matrixId, int nodeType, int count, Map<Integer, Integer> countPerPart) {
		super(matrixId);
		this.nodeType = nodeType;
		this.count = count;
		this.countPerPart = countPerPart;
	}

	public int getNodeType() {
		return nodeType;
	}

	public int getCount() {
		return count;
	}

	public Map<Integer, Integer> getCountPerPart() {
		return countPerPart;
	}

	@Override
	public List<PartitionGetParam> split() {
		List<PartitionGetParam> partParams = new ArrayList<>();
		List<PartitionKey> partitions =
						PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

		for (PartitionKey partitionKey : partitions) {
			int count = countPerPart.getOrDefault(partitionKey.getPartitionId(), 0);
			if (count > 0) {
				partParams.add(new PartSampleEdgeParam(matrixId, partitionKey, nodeType, count));
			}
		}

		return partParams;
	}
}
