package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;

import java.util.List;

public class SampleNode extends GetFunc {
	/**
	 * Create a new DefaultGetFunc.
	 *
	 * @param param parameter of get udf
	 */
	public SampleNode(SampleNodeParam param) {
		super(param);
	}

	/**
	 * Create a empty SampleNode
	 */
	public SampleNode() {
		super(null);
	}


	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartSampleNodeParam param = (PartSampleNodeParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		return new PartSampleNodeResult(part.getPartitionKey().getPartitionId(), row.sampleNode(param.getNodeType(), param.getCount()));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int count = ((SampleNodeParam) param).getCount();
		long[] nodeIds = new long[count];

		int i = 0;
		for (PartitionGetResult result : partResults) {
			PartSampleNodeResult sampleNodeResult = (PartSampleNodeResult) result;
			for (long nodeId : sampleNodeResult.getNodeIds()) {
				nodeIds[i] = nodeId;
				i++;
			}
		}

		return new SampleNodeResult(nodeIds);
	}
}
