package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.BinaryFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNodeBinaryFeatureResult extends PartitionGetResult {
	private int partId;
	private BinaryFeatures[] nodeFeatures;

	PartGetNodeBinaryFeatureResult(int partId, BinaryFeatures[] nodeFeatures) {
		this.partId = partId;
		this.nodeFeatures = nodeFeatures;
	}

	public PartGetNodeBinaryFeatureResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(nodeFeatures.length);

		for (BinaryFeatures nodeFeature : nodeFeatures) {
			int[] featureSizes = nodeFeature.getFeatureSizes();
			Byte[] featureValues = nodeFeature.getFeatureValues();

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
		nodeFeatures = new BinaryFeatures[buf.readInt()];

		for (int i = 0; i < nodeFeatures.length; i++) {
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

			nodeFeatures[i] = new BinaryFeatures(featureSizes, featureValues);
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (BinaryFeatures nodeFeature : nodeFeatures) {
			len += 8;
			len += nodeFeature.getFeatureSizes().length * 4;
			len += nodeFeature.getFeatureValues().length;
		}
		return len;
	}

	public BinaryFeatures[] getNodeFeatures() {
		return nodeFeatures;
	}

	public int getPartId() {
		return partId;
	}
}
