package com.tencent.angel.graph.client.sampleedge;

import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleEdgeResult extends PartitionGetResult {
	private int partId;
	private EdgeId[] edgeIds;

	public PartSampleEdgeResult(int partId, EdgeId[] edgeIds) {
		this.partId = partId;
		this.edgeIds = edgeIds;
	}

	public PartSampleEdgeResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(edgeIds.length);
		for (EdgeId edgeId : edgeIds) {
			buf.writeLong(edgeId.getFromNodeId());
			buf.writeLong(edgeId.getToNodeId());
			buf.writeInt(edgeId.getType());
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		int len = buf.readInt();
		edgeIds = new EdgeId[len];

		for (int i = 0; i < len; i++) {
			long fromNodeId = buf.readLong();
			long toNodeId = buf.readLong();
			int type = buf.readInt();
			edgeIds[i] = new EdgeId(fromNodeId, toNodeId, type);
		}
	}

	@Override
	public int bufferLen() {
		return 4 + 20 * edgeIds.length;
	}

	public EdgeId[] getEdgeIds() {
		return edgeIds;
	}

	public int getPartId() {
		return partId;
	}
}
