package com.tencent.angel.graph.client.sampleedge;

import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class SampleEdgeResult extends GetResult {
	private EdgeId[] edgeIds;

	public SampleEdgeResult(EdgeId[] edgeIds) {
		this.edgeIds = edgeIds;
	}

	public EdgeId[] getEdgeIds() {
		return edgeIds;
	}

	public void setEdgeIds(EdgeId[] edgeIds) {
		this.edgeIds = edgeIds;
	}
}
