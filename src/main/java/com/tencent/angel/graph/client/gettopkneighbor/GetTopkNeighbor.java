package com.tencent.angel.graph.client.gettopkneighbor;

import com.tencent.angel.graph.data.NodeIDWeightPair;
import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.graph.client.getfullneighbor.GetFullNeighborParam;
import com.tencent.angel.graph.client.getfullneighbor.PartGetFullNeighborResult;
import com.tencent.angel.graph.data.graph.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.*;

public class GetTopkNeighbor extends GetFunc {
	private static final NodeIdWeightPairComparator NODE_COMPARATOR = new NodeIdWeightPairComparator();

	/**
	 * Create a new DefaultGetFunc.
	 *
	 * @param param parameter of get udf
	 */
	public GetTopkNeighbor(GetTopkNeighborParam param) {
		super(param);
	}

	public GetTopkNeighbor() {
		super(null);
	}

	@Override
	public PartitionGetResult partitionGet(PartitionGetParam partParam) {
		PartGetTopkNeighborParam param = (PartGetTopkNeighborParam) partParam;
		ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
		RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
		GraphServerRow row = (GraphServerRow) part.getRow(0);

		// Results
		NodeIDWeightPairs[] results = new NodeIDWeightPairs[param.getNodeIds().length];

		// Get neighbors for each node
		long[] nodeIds = param.getNodeIds();
		for (int i = 0; i < nodeIds.length; i++) {
			results[i] = getNeighbors(row.getNode(nodeIds[i]), param.getEdgeTypes(), param.getK());
		}

		return new PartGetTopkNeighborResult(part.getPartitionKey().getPartitionId(), results);
	}

	private NodeIDWeightPairs getNeighbors(Node node, int[] edgeTypes, int k) {
		if (node == null) {
			return NodeIDWeightPairs.empty();
		}

		PriorityQueue<NodeIDWeightPair> minHeap = new PriorityQueue<>(NODE_COMPARATOR);
		for (int i = 0; i < edgeTypes.length; i++) {
			int edgeType = edgeTypes[i];
			if (edgeType >= 0 && edgeType < node.getEdgeTypes().length) {
				// First get store position for this edge type
				int startIndex = edgeType > 0 ? node.getNeigborGroupIndices()[i - 1] : 0;
				int endIndex = node.getNeigborGroupIndices()[edgeType];
				int len = endIndex - startIndex;

				// Get neighbor node weight
				for (int j = 0; j < len; j++) {
					long nodeId = node.getNeighbors()[startIndex + j];
					float preSumWeight = (startIndex + j) == 0 ? 0 : node.getNeighborAccSumWeights()[startIndex + j - 1];
					float weight = node.getNeighborAccSumWeights()[startIndex + j] - preSumWeight;

					if (minHeap.size() < k) {
						minHeap.add(new NodeIDWeightPair(edgeType, nodeId, weight));
					} else {
						if (minHeap.peek().getNodeWeight() < weight) {
							minHeap.poll();
							minHeap.add(new NodeIDWeightPair(edgeType, nodeId, weight));
						}
					}
				}
			}
		}

		int neighborsNum = minHeap.size();

		// Valid edge types
		int[] validEdgeTypes = new int[neighborsNum];

		// Neighbors
		long[] neighborNodeIds = new long[neighborsNum];

		// Neighbors weights
		float[] neighborWeights = new float[neighborsNum];

		int i = neighborsNum - 1;
		while (!minHeap.isEmpty()) {
			NodeIDWeightPair nodeIDWeightPair = minHeap.poll();
			validEdgeTypes[i] = nodeIDWeightPair.getEdgeType();
			neighborNodeIds[i] = nodeIDWeightPair.getNodeId();
			neighborWeights[i] = nodeIDWeightPair.getNodeWeight();
			i--;
		}

		return new NodeIDWeightPairs(validEdgeTypes, neighborNodeIds, neighborWeights);
	}

	@Override
	public GetResult merge(List<PartitionGetResult> partResults) {
		int[] offsets = ((GetTopkNeighborParam) param).getOffsets();
		long[] nodeIds = ((GetTopkNeighborParam) param).getNodeIds();
		int len = ((GetTopkNeighborParam) param).getNodeIds().length;

		Map<Long, NodeIDWeightPairs> neighbors = new Long2ObjectOpenHashMap<>(len);
		for (PartitionGetResult result : partResults) {
			PartGetTopkNeighborResult getNeighborResult = (PartGetTopkNeighborResult) result;
			NodeIDWeightPairs[] nodeResults = getNeighborResult.getNeighborIndices();
			int startIndex = getNeighborResult.getPartId() == 0 ? 0 : offsets[getNeighborResult.getPartId() - 1];
			int endIndex = offsets[getNeighborResult.getPartId()];
			for (int i = startIndex; i < endIndex; i++) {
				neighbors.put(nodeIds[i], nodeResults[i - startIndex]);
			}
		}

		return new GetTopkNeighborResult(neighbors);
	}

	static class NodeIdWeightPairComparator implements Comparator<NodeIDWeightPair> {
		@Override
		public int compare(NodeIDWeightPair o1, NodeIDWeightPair o2) {
			return Float.compare(o1.getNodeWeight(), o2.getNodeWeight());
		}
	}
}
