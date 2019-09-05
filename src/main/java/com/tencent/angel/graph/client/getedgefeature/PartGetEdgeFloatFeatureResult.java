package com.tencent.angel.graph.client.getedgefeature;

import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetEdgeFloatFeatureResult extends PartitionGetResult {
	private int partId;
	private FloatFeatures[] edgeFeatures;

	public PartGetEdgeFloatFeatureResult(int partId, FloatFeatures[] edgeFeatures) {
		this.partId = partId;
		this.edgeFeatures = edgeFeatures;
	}

	public PartGetEdgeFloatFeatureResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(edgeFeatures.length);

		for (FloatFeatures edgeFeature : edgeFeatures) {
			int[] featureSizes = edgeFeature.getFeatureSizes();
			Float[] featureValues = edgeFeature.getFeatureValues();

			buf.writeInt(featureSizes.length);
			for (int featureSize : featureSizes) {
				buf.writeInt(featureSize);
			}

			buf.writeInt(featureValues.length);
			for (Float featureValue : featureValues) {
				buf.writeFloat(featureValue);
			}
		}
	}

	@Override
	public void deserialize(ByteBuf buf) {
		partId = buf.readInt();
		edgeFeatures = new FloatFeatures[buf.readInt()];

		for (int i = 0; i < edgeFeatures.length; i++) {
			int featureNum = buf.readInt();

			int[] featureSizes = new int[featureNum];
			for (int s = 0; s < featureNum; s++) {
				featureSizes[s] = buf.readInt();
			}

			int featureValueSize = buf.readInt();
			Float[] featureValues = new Float[featureNum];
			for (int v = 0; v < featureValueSize; v++) {
				featureValues[v] = buf.readFloat();
			}

			edgeFeatures[i] = new FloatFeatures(featureSizes, featureValues);
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (FloatFeatures edgeFeature : edgeFeatures) {
			len += 8;
			len += edgeFeature.getFeatureSizes().length * 4;
			len += edgeFeature.getFeatureValues().length * 4;
		}
		return len;
	}

	public FloatFeatures[] getEdgeFeatures() {
		return edgeFeatures;
	}

	public int getPartId() {
		return partId;
	}
}
