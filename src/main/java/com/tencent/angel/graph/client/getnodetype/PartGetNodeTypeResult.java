package com.tencent.angel.graph.client.getnodetype;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNodeTypeResult extends PartitionGetResult {
	private int partId;
	private Integer[] nodeTypes;

	public PartGetNodeTypeResult(int partId, Integer[] nodeTypes) {
		this.partId = partId;
		this.nodeTypes = nodeTypes;
	}

	public PartGetNodeTypeResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(nodeTypes.length);
		for (Integer nodeType: nodeTypes) {
			buf.writeInt(nodeType);
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		partId = buf.readInt();
		int size = buf.readInt();
		nodeTypes = new Integer[size];
		for (int i = 0; i < size; i++) {
			nodeTypes[i] = buf.readInt();
		}
	}

	@Override
	public int bufferLen() {
		return 8 + nodeTypes.length * 4;
	}

	public Integer[] getNodeTypes() {
		return nodeTypes;
	}

	public int getPartId() {
		return partId;
	}
}
