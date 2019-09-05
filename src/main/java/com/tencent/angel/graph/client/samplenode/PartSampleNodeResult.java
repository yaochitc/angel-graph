package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeResult extends PartitionGetResult {
	private int partId;
	private long[] nodeIds;

	public PartSampleNodeResult(int partId, long[] nodeIds) {
		this.partId = partId;
		this.nodeIds = nodeIds;
	}

	public PartSampleNodeResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(nodeIds.length);
		for (long nodeId : nodeIds) {
			buf.writeLong(nodeId);
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		int len = buf.readInt();
		nodeIds = new long[len];
		for (int i = 0; i < len; i++) {
			nodeIds[i] = buf.readLong();
		}
	}

	@Override
	public int bufferLen() {
		return 4 + 8 * nodeIds.length;
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public int getPartId() {
		return partId;
	}
}
