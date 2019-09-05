package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetEdgeLongFeatureResult extends GetResult {
	private Map<EdgeId, LongFeatures> edgeFeatures;

	GetEdgeLongFeatureResult(Map<EdgeId, LongFeatures> edgeFeatures) {
		this.edgeFeatures = edgeFeatures;
	}

	public Map<EdgeId, LongFeatures> getEdgeFeatures() {
		return edgeFeatures;
	}

	public void setEdgeFeatures(Map<EdgeId, LongFeatures> edgeFeatures) {
		this.edgeFeatures = edgeFeatures;
	}
}
