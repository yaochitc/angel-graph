package com.tencent.angel.graph.client.getnodetype;

import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetNodeType extends GetFunc {
	/**
	 * Create a new DefaultGetFunc.
	 *
	 * @param param parameter of get udf
	 */
	public GetNodeType(GetParam param) {
		super(param);
	}

	public GetNodeType() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetNodeTypeParam param = (PartGetNodeTypeParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		Integer[] results = new Integer[param.getNodeIds().length];

		// Get neighbors for each node
		long[] nodeIds = param.getNodeIds();
		for (int i = 0; i < nodeIds.length; i++) {
			Node node = row.getNode(nodeIds[i]);
			results[i] = null != node ? node.getType() : -1;
		}

		return new PartGetNodeTypeResult(part.getPartitionKey().getPartitionId(), results);
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetNodeTypeParam) param).getOffsets();
		long[] nodeIds = ((GetNodeTypeParam) param).getNodeIds();
		int len = ((GetNodeTypeParam) param).getNodeIds().length;

		Map<Long, Integer> nodeIdToTypes = new Long2IntOpenHashMap(len);
		for (PartitionGetResult result : partResults) {
			PartGetNodeTypeResult getNodeTypeResult = (PartGetNodeTypeResult) result;
			Integer[] nodeResults = getNodeTypeResult.getNodeTypes();
			int startIndex = getNodeTypeResult.getPartId() == 0 ? 0 : offsets[getNodeTypeResult.getPartId() - 1];
			int endIndex = offsets[getNodeTypeResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				nodeIdToTypes.put(nodeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new GetNodeTypeResult(nodeIdToTypes);
	}
}
