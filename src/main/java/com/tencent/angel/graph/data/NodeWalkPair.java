package com.tencent.angel.graph.data;

import java.util.Map;

public class NodeWalkPair {
	private Map<Long, long[]> nodeNeighborIds;

	private Map<Long, NodeIDWeightPairs> parentNodeNeighbors;

	public NodeWalkPair(Map<Long, long[]> nodeNeighborIds, Map<Long, NodeIDWeightPairs> parentNodeNeighbors) {
		this.nodeNeighborIds = nodeNeighborIds;
		this.parentNodeNeighbors = parentNodeNeighbors;
	}

	public Map<Long, long[]> getNodeNeighborIds() {
		return nodeNeighborIds;
	}

	public void setNodeNeighborIds(Map<Long, long[]> nodeNeighborIds) {
		this.nodeNeighborIds = nodeNeighborIds;
	}

	public Map<Long, NodeIDWeightPairs> getParentNodeNeighbors() {
		return parentNodeNeighbors;
	}

	public void setParentNodeNeighbors(Map<Long, NodeIDWeightPairs> parentNodeNeighbors) {
		this.parentNodeNeighbors = parentNodeNeighbors;
	}
}
