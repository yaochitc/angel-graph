package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetEdgeLongFeatureResult extends PartitionGetResult {
	private int partId;
	private LongFeatures[] edgeFeatures;

	PartGetEdgeLongFeatureResult(int partId, LongFeatures[] edgeFeatures) {
		this.partId = partId;
		this.edgeFeatures = edgeFeatures;
	}

	public PartGetEdgeLongFeatureResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(edgeFeatures.length);

		for (LongFeatures edgeFeature : edgeFeatures) {
			int[] featureSizes = edgeFeature.getFeatureSizes();
			Long[] featureValues = edgeFeature.getFeatureValues();

			buf.writeInt(featureSizes.length);
			for (int featureSize : featureSizes) {
				buf.writeInt(featureSize);
			}

			buf.writeInt(featureValues.length);
			for (Long featureValue : featureValues) {
				buf.writeLong(featureValue);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		partId = buf.readInt();
		edgeFeatures = new LongFeatures[buf.readInt()];

		for (int i = 0; i < edgeFeatures.length; i++) {
			int featureNum = buf.readInt();

			int[] featureSizes = new int[featureNum];
			for (int s = 0; s < featureNum; s++) {
				featureSizes[s] = buf.readInt();
			}

			int featureValueSize = buf.readInt();
			Long[] featureValues = new Long[featureNum];
			for (int v = 0; v < featureValueSize; v++) {
				featureValues[v] = buf.readLong();
			}

			edgeFeatures[i] = new LongFeatures(featureSizes, featureValues);
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (LongFeatures edgeFeature : edgeFeatures) {
			len += 8;
			len += edgeFeature.getFeatureSizes().length * 4;
			len += edgeFeature.getFeatureValues().length * 8;
		}
		return len;
	}

	public LongFeatures[] getEdgeFeatures() {
		return edgeFeatures;
	}

	public int getPartId() {
		return partId;
	}
}
