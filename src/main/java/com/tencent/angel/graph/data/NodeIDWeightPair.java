package com.tencent.angel.graph.data;

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

public class NodeIDWeightPair implements Serialize {

	/**
	 * Edge type
	 */
	private int edgeType;

	/**
	 * Neighbor node id
	 */
	private long nodeId;

	/**
	 * Neighbor node weight
	 */
	private float nodeWeight;

	public NodeIDWeightPair(int edgeType, long nodeId, float nodeWeight) {
		this.edgeType = edgeType;
		this.nodeId = nodeId;
		this.nodeWeight = nodeWeight;
	}

	public int getEdgeType() {
		return edgeType;
	}

	public void setEdgeType(int edgeType) {
		this.edgeType = edgeType;
	}

	public long getNodeId() {
		return nodeId;
	}

	public float getNodeWeight() {
		return nodeWeight;
	}

	@Override
	public void serialize(ByteBuf output) {
		output.writeInt(edgeType);
		output.writeLong(nodeId);
		output.writeFloat(nodeWeight);
	}

	@Override
	public void deserialize(ByteBuf input) {
		edgeType = input.readInt();
		nodeId = input.readLong();
		nodeWeight = input.readFloat();
	}

	@Override
	public int bufferLen() {
		return 16;
	}
}
