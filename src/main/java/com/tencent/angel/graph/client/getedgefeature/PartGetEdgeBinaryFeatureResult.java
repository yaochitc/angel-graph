package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetEdgeBinaryFeatureResult extends PartitionGetResult {
	private int partId;
	private BinaryFeatures[] edgeFeatures;

	PartGetEdgeBinaryFeatureResult(int partId, BinaryFeatures[] edgeFeatures) {
		this.partId = partId;
		this.edgeFeatures = edgeFeatures;
	}

	public PartGetEdgeBinaryFeatureResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(edgeFeatures.length);

		for (BinaryFeatures edgeFeature : edgeFeatures) {
			int[] featureSizes = edgeFeature.getFeatureSizes();
			Byte[] featureValues = edgeFeature.getFeatureValues();

			buf.writeInt(featureSizes.length);
			for (int featureSize : featureSizes) {
				buf.writeInt(featureSize);
			}

			buf.writeInt(featureValues.length);
			for (Byte featureValue : featureValues) {
				buf.writeByte(featureValue);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		partId = buf.readInt();
		edgeFeatures = new BinaryFeatures[buf.readInt()];

		for (int i = 0; i < edgeFeatures.length; i++) {
			int featureNum = buf.readInt();

			int[] featureSizes = new int[featureNum];
			for (int s = 0; s < featureNum; s++) {
				featureSizes[s] = buf.readInt();
			}

			int featureValueSize = buf.readInt();
			Byte[] featureValues = new Byte[featureNum];
			for (int v = 0; v < featureValueSize; v++) {
				featureValues[v] = buf.readByte();
			}

			edgeFeatures[i] = new BinaryFeatures(featureSizes, featureValues);
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (BinaryFeatures edgeFeature : edgeFeatures) {
			len += 8;
			len += edgeFeature.getFeatureSizes().length * 4;
			len += edgeFeature.getFeatureValues().length;
		}
		return len;
	}

	public BinaryFeatures[] getEdgeFeatures() {
		return edgeFeatures;
	}

	public int getPartId() {
		return partId;
	}
}
