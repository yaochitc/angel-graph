package com.tencent.angel.graph.client.buildsampler;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class BuildSamplerResult extends GetResult {
	private Map<Integer, Map<Integer, Float>> nodeWeightSum;
	private Map<Integer, Map<Integer, Float>> edgeWeightSum;

	public BuildSamplerResult(Map<Integer, Map<Integer, Float>> nodeWeightSum,
														Map<Integer, Map<Integer, Float>> edgeWeightSum) {
		this.nodeWeightSum = nodeWeightSum;
		this.edgeWeightSum = edgeWeightSum;
	}

	public Map<Integer, Map<Integer, Float>> getNodeWeightSum() {
		return nodeWeightSum;
	}

	public void setNodeWeightSum(Map<Integer, Map<Integer, Float>> nodeWeightSum) {
		this.nodeWeightSum = nodeWeightSum;
	}

	public Map<Integer, Map<Integer, Float>> getEdgeWeightSum() {
		return edgeWeightSum;
	}

	public void setEdgeWeightSum(Map<Integer, Map<Integer, Float>> edgeWeightSum) {
		this.edgeWeightSum = edgeWeightSum;
	}
}
