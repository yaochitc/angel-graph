package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.FloatFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNodeFloatFeatureResult extends PartitionGetResult {
	private int partId;
	private FloatFeatures[] nodeFeatures;

	public PartGetNodeFloatFeatureResult(int partId, FloatFeatures[] nodeFeatures) {
		this.partId = partId;
		this.nodeFeatures = nodeFeatures;
	}

	public PartGetNodeFloatFeatureResult() {
		this(-1, null);
	}


	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(nodeFeatures.length);

		for (FloatFeatures nodeFeature : nodeFeatures) {
			int[] featureSizes = nodeFeature.getFeatureSizes();
			Float[] featureValues = nodeFeature.getFeatureValues();

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
		nodeFeatures = new FloatFeatures[buf.readInt()];

		for (int i = 0; i < nodeFeatures.length; i++) {
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

			nodeFeatures[i] = new FloatFeatures(featureSizes, featureValues);
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (FloatFeatures nodeFeature : nodeFeatures) {
			len += 8;
			len += nodeFeature.getFeatureSizes().length * 4;
			len += nodeFeature.getFeatureValues().length * 4;
		}
		return len;
	}

	public FloatFeatures[] getNodeFeatures() {
		return nodeFeatures;
	}

	public int getPartId() {
		return partId;
	}
}
