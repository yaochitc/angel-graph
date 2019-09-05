package com.tencent.angel.graph.client.getnodetype;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetNodeTypeResult extends GetResult {
	/**
	 * Node id to node type map
	 */
	private Map<Long, Integer> nodeIdToNodeTypes;

	GetNodeTypeResult(Map<Long, Integer> nodeIdToNodeTypes) {
		this.nodeIdToNodeTypes = nodeIdToNodeTypes;
	}

	public Map<Long, Integer> getNodeIdToNodeTypes() {
		return nodeIdToNodeTypes;
	}

	public void setNodeIdToNodeTypes(
					Map<Long, Integer> nodeIdToNeighbors) {
		this.nodeIdToNodeTypes = nodeIdToNeighbors;
	}
}
