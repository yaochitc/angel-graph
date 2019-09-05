package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

public class SampleNodeResult extends GetResult {
	private long[] nodeIds;

	public SampleNodeResult(long[] nodeIds) {
		this.nodeIds = nodeIds;
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public void setNodeIds(long[] nodeIds) {
		this.nodeIds = nodeIds;
	}
}
