package com.tencent.angel.graph.client.getnodetype;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNodeTypeParam extends PartitionGetParam {
	/**
	 * Node ids: it just a view for original node ids
	 */
	private long[] nodeIds;

	/**
	 * Store position: start index in nodeIds
	 */
	private int startIndex;

	/**
	 * Store position: end index in nodeIds
	 */
	private int endIndex;

	public PartGetNodeTypeParam(int matrixId, PartitionKey part, long[] nodeIds
					, int startIndex, int endIndex) {
		super(matrixId, part);
		this.nodeIds = nodeIds;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public PartGetNodeTypeParam() {
		this(0, null, null, 0, 0);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public int getStartIndex() {
		return startIndex;
	}

	public int getEndIndex() {
		return endIndex;
	}

	@Override
	public void serialize(ByteBuf buf) {
		super.serialize(buf);
		buf.writeInt(endIndex - startIndex);

		for (int i = startIndex; i < endIndex; i++) {
			buf.writeLong(nodeIds[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		int len = buf.readInt();
		nodeIds = new long[len];
		for (int i = 0; i < len; i++) {
			nodeIds[i] = buf.readLong();
		}
	}

	@Override
	public int bufferLen() {
		return super.bufferLen() + 4 + 8 * nodeIds.length;
	}
}
