package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNodeFeatureParam extends PartitionGetParam {
	/**
	 * Node ids: it just a view for original node ids
	 */
	private long[] nodeIds;

	/**
	 * Feature ids
	 */
	private int[] fids;

	/**
	 * Store position: start index in nodeIds
	 */
	private int startIndex;

	/**
	 * Store position: end index in nodeIds
	 */
	private int endIndex;

	public PartGetNodeFeatureParam(int matrixId, PartitionKey part, long[] nodeIds, int[] fids
					, int startIndex, int endIndex) {
		super(matrixId, part);
		this.nodeIds = nodeIds;
		this.fids = fids;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public PartGetNodeFeatureParam() {
		this(0, null, null, null, 0, 0);
	}

	public long[] getNodeIds() {
		return nodeIds;
	}

	public int[] getFids() {
		return fids;
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

		buf.writeInt(fids.length);
		for (int i = 0; i < fids.length; i++) {
			buf.writeInt(fids[i]);
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		super.deserialize(buf);
		int nodeLen = buf.readInt();
		nodeIds = new long[nodeLen];
		for (int i = 0; i < nodeLen; i++) {
			nodeIds[i] = buf.readLong();
		}

		int fidLen = buf.readInt();
		fids = new int[fidLen];
		for (int i = 0; i < fidLen; i++) {
			fids[i] = buf.readInt();
		}
	}

	@Override
	public int bufferLen() {
		return super.bufferLen() + 4 + 8 * nodeIds.length + 4 + 4 * fids.length;
	}
}
