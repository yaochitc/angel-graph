package com.tencent.angel.graph.client.buildsampler;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;

import java.util.Map;

public class PartBuildSamplerResult extends PartitionGetResult {
	private int partId;
	private Map<Integer, Float> nodeWeightSums;
	private Map<Integer, Float> edgeWeightSums;

	public PartBuildSamplerResult(int partId, Map<Integer, Float> nodeWeightSums,
																Map<Integer, Float> edgeWeightSums) {
		this.partId = partId;
		this.nodeWeightSums = nodeWeightSums;
		this.edgeWeightSums = edgeWeightSums;
	}


	public PartBuildSamplerResult() {
		this(-1, null, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);

		buf.writeInt(nodeWeightSums.size());
		for (Map.Entry<Integer, Float> entry : nodeWeightSums.entrySet()) {
			buf.writeInt(entry.getKey());
			buf.writeFloat(entry.getValue());
		}

		buf.writeInt(edgeWeightSums.size());
		for (Map.Entry<Integer, Float> entry : edgeWeightSums.entrySet()) {
			buf.writeInt(entry.getKey());
			buf.writeFloat(entry.getValue());
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		partId = buf.readInt();

		int size = buf.readInt();
		nodeWeightSums = new Int2FloatOpenHashMap();
		for (int i = 0; i < size; i++) {
			int type = buf.readInt();
			float weight = buf.readFloat();
			nodeWeightSums.put(type, weight);
		}

		size = buf.readInt();
		edgeWeightSums = new Int2FloatOpenHashMap();
		for (int i = 0; i < size; i++) {
			int type = buf.readInt();
			float weight = buf.readFloat();
			edgeWeightSums.put(type, weight);
		}
	}

	@Override
	public int bufferLen() {
		return 8 + 8 * nodeWeightSums.size() + 4 + 8 * edgeWeightSums.size();
	}

	public Map<Integer, Float> getNodeWeightSums() {
		return nodeWeightSums;
	}

	public Map<Integer, Float> getEdgeWeightSums() {
		return edgeWeightSums;
	}

	public int getPartId() {
		return partId;
	}
}
