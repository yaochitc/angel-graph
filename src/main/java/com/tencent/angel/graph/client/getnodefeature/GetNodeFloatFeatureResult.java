package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetNodeFloatFeatureResult extends GetResult {
	private Map<Long, FloatFeatures> nodeFeatures;

	GetNodeFloatFeatureResult(Map<Long, FloatFeatures> nodeFeatures) {
		this.nodeFeatures = nodeFeatures;
	}

	public Map<Long, FloatFeatures> getNodeFeatures() {
		return nodeFeatures;
	}

	public void setNodeFeatures(Map<Long, FloatFeatures> nodeFeatures) {
		this.nodeFeatures = nodeFeatures;
	}
}
