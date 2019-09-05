package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class GetEdgeBinaryFeatureResult extends GetResult {
	private Map<EdgeId, BinaryFeatures> edgeFeatures;

	GetEdgeBinaryFeatureResult(Map<EdgeId, BinaryFeatures> edgeFeatures) {
		this.edgeFeatures = edgeFeatures;
	}

	public Map<EdgeId, BinaryFeatures> getEdgeFeatures() {
		return edgeFeatures;
	}

	public void setEdgeFeatures(Map<EdgeId, BinaryFeatures> edgeFeatures) {
		this.edgeFeatures = edgeFeatures;
	}
}
