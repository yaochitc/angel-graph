package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetNodeLongFeatureResult extends GetResult {
	private Map<Long, LongFeatures> nodeFeatures;

	GetNodeLongFeatureResult(Map<Long, LongFeatures> nodeFeatures) {
		this.nodeFeatures = nodeFeatures;
	}

	public Map<Long, LongFeatures> getNodeFeatures() {
		return nodeFeatures;
	}

	public void setNodeFeatures(Map<Long, LongFeatures> nodeFeatures) {
		this.nodeFeatures = nodeFeatures;
	}
}
