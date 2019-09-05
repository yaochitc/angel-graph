package com.tencent.angel.graph.client.buildsampler;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class BuildSamplerParam extends GetParam {

	public BuildSamplerParam(int matrixId) {
		super(matrixId);
	}

	@Override
	public List<PartitionGetParam> split() {
		List<PartitionGetParam> partParams = new ArrayList<>();
		List<PartitionKey> partitions =
						PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

		for (PartitionKey partitionKey : partitions) {
			partParams.add(new PartitionGetParam(matrixId, partitionKey));
		}

		return partParams;
	}

}
