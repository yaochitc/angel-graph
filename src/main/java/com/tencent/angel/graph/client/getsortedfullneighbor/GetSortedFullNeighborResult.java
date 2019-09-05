package com.tencent.angel.graph.client.getsortedfullneighbor;

import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetSortedFullNeighborResult extends GetResult {
	/**
	 * Node id to neighbors map
	 */
	private Map<Long, NodeIDWeightPairs> nodeIdToNeighbors;

	GetSortedFullNeighborResult(Map<Long, NodeIDWeightPairs> nodeIdToNeighbors) {
		this.nodeIdToNeighbors = nodeIdToNeighbors;
	}

	public Map<Long, NodeIDWeightPairs> getNodeIdToNeighbors() {
		return nodeIdToNeighbors;
	}

	public void setNodeIdToNeighbors(
					Map<Long, NodeIDWeightPairs> nodeIdToNeighbors) {
		this.nodeIdToNeighbors = nodeIdToNeighbors;
	}
}
