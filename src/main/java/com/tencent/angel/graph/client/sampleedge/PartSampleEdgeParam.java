package com.tencent.angel.graph.client.sampleedge;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleEdgeParam extends PartitionGetParam {
	private int edgeType;

	private int count;

	public PartSampleEdgeParam(int matrixId, PartitionKey part, int edgeType, int count) {
		super(matrixId, part);
		this.edgeType = edgeType;
		this.count = count;
	}

	public PartSampleEdgeParam() {
		this(0, null, 0, 0);
	}

	public int getEdgeType() {
		return edgeType;
	}

	public int getCount() {
		return count;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(edgeType);
		buf.writeInt(count);
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		edgeType = buf.readInt();
		count = buf.readInt();
	}

	@Override
	public int bufferLen() {
		return super.bufferLen() + 8;
	}
}
