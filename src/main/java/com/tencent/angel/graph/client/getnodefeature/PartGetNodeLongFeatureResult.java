package com.tencent.angel.graph.client.getnodefeature;

import com.tencent.angel.graph.data.feature.LongFeatures;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNodeLongFeatureResult extends PartitionGetResult {
	private int partId;
	private LongFeatures[] nodeFeatures;

	PartGetNodeLongFeatureResult(int partId, LongFeatures[] nodeFeatures) {
		this.partId = partId;
		this.nodeFeatures = nodeFeatures;
	}

	public PartGetNodeLongFeatureResult() {
		this(-1, null);
	}

	@Override
	public void serialize(ByteBuf buf) {
		buf.writeInt(partId);
		buf.writeInt(nodeFeatures.length);

		for (LongFeatures nodeFeature : nodeFeatures) {
			int[] featureSizes = nodeFeature.getFeatureSizes();
			Long[] featureValues = nodeFeature.getFeatureValues();

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
		nodeFeatures = new LongFeatures[buf.readInt()];

		for (int i = 0; i < nodeFeatures.length; i++) {
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

			nodeFeatures[i] = new LongFeatures(featureSizes, featureValues);
		}
	}

	@Override
	public int bufferLen() {
		int len = 8;
		for (LongFeatures nodeFeature : nodeFeatures) {
			len += 8;
			len += nodeFeature.getFeatureSizes().length * 4;
			len += nodeFeature.getFeatureValues().length * 8;
		}
		return len;
	}

	public LongFeatures[] getNodeFeatures() {
		return nodeFeatures;
	}

	public int getPartId() {
		return partId;
	}

}
