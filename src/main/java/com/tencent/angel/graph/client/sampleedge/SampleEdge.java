package com.tencent.angel.graph.client.sampleedge;

import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;

import java.util.List;

public class SampleEdge extends GetFunc {
	/**
	 * Create a new DefaultGetFunc.
	 *
	 * @param param parameter of get udf
	 */
	public SampleEdge(SampleEdgeParam param) {
		super(param);
	}

	/**
	 * Create a empty SampleNode
	 */
	public SampleEdge() {
		super(null);
	}


	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartSampleEdgeParam param = (PartSampleEdgeParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		return new PartSampleEdgeResult(part.getPartitionKey().getPartitionId(), row.sampleEdge(param.getEdgeType(), param.getCount()));
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int count = ((SampleEdgeParam) param).getCount();
		EdgeId[] edgeIds = new EdgeId[count];

		int i = 0;
		for (PartitionGetResult result : partResults) {
			PartSampleEdgeResult sampleNodeResult = (PartSampleEdgeResult) result;
			for (EdgeId edgeId: sampleNodeResult.getEdgeIds()) {
				edgeIds[i] = edgeId;
				i++;
			}
		}

		return new SampleEdgeResult(edgeIds);
	}
}
