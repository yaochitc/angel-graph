package com.tencent.angel.graph.client.samplenode;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeParam extends PartitionGetParam {
	private int nodeType;

	private int count;

	public PartSampleNodeParam(int matrixId, PartitionKey part, int nodeType, int count) {
		super(matrixId, part);
		this.nodeType = nodeType;
		this.count = count;
	}

	public PartSampleNodeParam() {
		this(0, null, 0, 0);
	}

	public int getNodeType() {
		return nodeType;
	}

	public int getCount() {
		return count;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(nodeType);
		buf.writeInt(count);
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		nodeType = buf.readInt();
		count = buf.readInt();
	}

	@Override
	public int bufferLen() {
		return super.bufferLen() + 8;
	}
}
