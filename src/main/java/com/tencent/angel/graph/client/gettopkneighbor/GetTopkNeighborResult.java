package com.tencent.angel.graph.client.gettopkneighbor;

import com.tencent.angel.graph.data.NodeIDWeightPairs;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetTopkNeighborResult extends GetResult {
	/**
	 * Node id to neighbors map
	 */
	private Map<Long, NodeIDWeightPairs> nodeIdToNeighbors;

	GetTopkNeighborResult(Map<Long, NodeIDWeightPairs> nodeIdToNeighbors) {
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
