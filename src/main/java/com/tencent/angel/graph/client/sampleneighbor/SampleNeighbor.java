package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.client.CompactSampler;
import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;
import java.util.Map;

public class SampleNeighbor extends GetFunc {

	/**
	 * Create SampleNeighbor
	 *
	 * @param param parameter
	 */
	public SampleNeighbor(SampleNeighborParam param) {
		super(param);
	}

	/**
	 * Create a empty SampleNeighbor
	 */
	public SampleNeighbor() {
		this(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartSampleNeighborParam param = (PartSampleNeighborParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		NodeIDWeightPairs[] results = new NodeIDWeightPairs[param.getNodeIds().length];

		// Get neighbors for each node
		long[] nodeIds = param.getNodeIds();
		for (int i = 0; i < nodeIds.length; i++) {
			results[i] = sampleNeighbor(row.getNode(nodeIds[i]), param.getEdgeTypes(), param.getCount());
		}

		return new PartSampleNeighborResult(part.getPartitionKey().getPartitionId(), results);
	}

	private NodeIDWeightPairs sampleNeighbor(Node node, int[] edgeTypes, int count) {
		if (node == null) {
			return NodeIDWeightPairs.empty();
		}

		// Edge type number to be sampled
		int edgeTypeNum = edgeTypes.length;

		// Rebuilt edge types and weights
		int[] subEdgeTypes;
		float[] subEdgeAccSumWeights;
		float subEdgeTotalSumWeights;

		if (edgeTypeNum < node.getEdgeTypes().length) {
			subEdgeTypes = new int[edgeTypeNum];
			subEdgeAccSumWeights = new float[edgeTypeNum];
			subEdgeTotalSumWeights = 0;
			for (int i = 0; i < edgeTypeNum; i++) {
				int edgeType = edgeTypes[i];
				subEdgeTypes[i] = edgeType;
				float edgeWeight = edgeType == 0 ? node.getEdgeAccSumWeights()[0] :
						node.getEdgeAccSumWeights()[edgeType] - node.getEdgeAccSumWeights()[edgeType - 1];
				subEdgeTotalSumWeights += edgeWeight;
				subEdgeAccSumWeights[i] = subEdgeTotalSumWeights;
			}
		} else {
			subEdgeTypes = node.getEdgeTypes();
			subEdgeAccSumWeights = node.getEdgeAccSumWeights();
			subEdgeTotalSumWeights = node.getEdgeTotalSumWeights();
		}

		if (subEdgeTotalSumWeights == 0) {
			return NodeIDWeightPairs.empty();
		}

		// Valid edge types
		int[] validEdgeTypes = new int[count];

		// Neighbors
		long[] neighborNodeIds = new long[count];

		// Neighbors weights
		float[] neighborWeights = new float[count];

		for (int i = 0; i < count; i++) {
			// sample edge type
			int edgeType = subEdgeTypes[CompactSampler.randomSelect(subEdgeAccSumWeights, 0, edgeTypeNum - 1)];

			// sample neighbor
			int startIndex = edgeType > 0 ? node.getNeigborGroupIndices()[edgeType - 1] : 0;
			int endIndex = node.getNeigborGroupIndices()[edgeType];
			int neighborNodeId = CompactSampler.randomSelect(node.getNeighborAccSumWeights(), startIndex, endIndex - 1);
			float preSumWeight = neighborNodeId <= 0 ? 0 : node.getNeighborAccSumWeights()[neighborNodeId - 1];

			validEdgeTypes[i] = edgeType;
			neighborNodeIds[i] = node.getNeighbors()[neighborNodeId];
			neighborWeights[i] = node.getNeighborAccSumWeights()[neighborNodeId] - preSumWeight;
		}

		return new NodeIDWeightPairs(validEdgeTypes, neighborNodeIds, neighborWeights);
	}


	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((SampleNeighborParam) param).getOffsets();
		long[] nodeIds = ((SampleNeighborParam) param).getNodeIds();
		int len = ((SampleNeighborParam) param).getNodeIds().length;

		Map<Long, NodeIDWeightPairs> neighbors = new Long2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartSampleNeighborResult getNeighborResult = (PartSampleNeighborResult) result;
			NodeIDWeightPairs[] nodeResults = getNeighborResult.getNeighborIndices();
			int startIndex = getNeighborResult.getPartId() == 0 ? 0 : offsets[getNeighborResult.getPartId() - 1];
			int endIndex = offsets[getNeighborResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				neighbors.put(nodeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new SampleNeighborResult(neighbors);
	}
}
