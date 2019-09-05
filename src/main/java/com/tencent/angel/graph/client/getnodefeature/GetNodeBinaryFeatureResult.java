package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetNodeBinaryFeatureResult extends GetResult {
	private Map<Long, BinaryFeatures> nodeFeatures;

	GetNodeBinaryFeatureResult(Map<Long, BinaryFeatures> nodeFeatures) {
		this.nodeFeatures = nodeFeatures;
	}

	public Map<Long, BinaryFeatures> getNodeFeatures() {
		return nodeFeatures;
	}

	public void setNodeFeatures(Map<Long, BinaryFeatures> nodeFeatures) {
		this.nodeFeatures = nodeFeatures;
	}
}
