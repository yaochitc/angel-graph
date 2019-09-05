package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetEdgeFloatFeatureResult extends GetResult {
	private Map<EdgeId, FloatFeatures> edgeFeatures;

	GetEdgeFloatFeatureResult(Map<EdgeId, FloatFeatures> edgeFeatures) {
		this.edgeFeatures = edgeFeatures;
	}

	public Map<EdgeId, FloatFeatures> getEdgeFeatures() {
		return edgeFeatures;
	}

	public void setEdgeFeatures(Map<EdgeId, FloatFeatures> edgeFeatures) {
		this.edgeFeatures = edgeFeatures;
	}
}
