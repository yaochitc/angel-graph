package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.graph.EdgeId;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetEdgeFeatureParam extends PartitionGetParam {
	/**
	 * Edge ids
	 */
	private EdgeId[] edgeIds;

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

	public PartGetEdgeFeatureParam(int matrixId, PartitionKey part, EdgeId[] edgeIds, int[] fids
					, int startIndex, int endIndex) {
		super(matrixId, part);
		this.edgeIds = edgeIds;
		this.fids = fids;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
	}

	public PartGetEdgeFeatureParam() {
		this(0, null, null, null, 0, 0);
	}

	public EdgeId[] getEdgeIds() {
		return edgeIds;
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
			EdgeId edgeId = edgeIds[i];
			buf.writeLong(edgeId.getFromNodeId());
			buf.writeLong(edgeId.getToNodeId());
			buf.writeInt(edgeId.getType());
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
		edgeIds = new EdgeId[nodeLen];

		for (int i = 0; i < nodeLen; i++) {
			long fromNodeId = buf.readLong();
			long toNodeId = buf.readLong();
			int type = buf.readInt();
			edgeIds[i] = new EdgeId(fromNodeId, toNodeId, type);
		}

		int fidLen = buf.readInt();
		fids = new int[fidLen];
		for (int i = 0; i < fidLen; i++) {
			fids[i] = buf.readInt();
		}
	}

	@Override
	public int bufferLen() {
		return super.bufferLen() + 4 + 20 * edgeIds.length + 4 + 4 * fids.length;
	}

}
