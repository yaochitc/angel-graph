package com.tencent.angel.graph.client;

import java.util.ArrayList;
import java.util.List;

public class CompactSampler<T> implements Sampler<T> {
	private List<T> itemList;
	private float[] accSumWeights;

	public void init(T[] items, float[] weights) {
		assert items.length == weights.length;

		int size = items.length;
		itemList = new ArrayList<>(size);
		accSumWeights = new float[size];
		float accSumWeight = 0f;
		for (int i = 0; i < size; i++) {
			itemList.add(items[i]);
			accSumWeight += weights[i];
			accSumWeights[i] = accSumWeight;
		}
	}

	public T sample() {
		int mid = randomSelect(accSumWeights, 0, itemList.size() - 1);
		return itemList.get(mid);
	}

	public static int randomSelect(float[] accSumWeights, int beginPos, int endPos) {
		float limitBegin = beginPos == 0 ? 0 : accSumWeights[beginPos - 1];
		float limitEnd = accSumWeights[endPos];
		float r = (float) Math.random() * (limitEnd - limitBegin) + limitBegin;

		int low = beginPos, high = endPos, mid = 0;
		boolean finish = false;
		while (low <= high && !finish) {
			mid = (low + high) / 2;
			float intervalBegin = mid == 0 ? 0 : accSumWeights[mid - 1];
			float intervalEnd = accSumWeights[mid];
			if (intervalBegin <= r && r < intervalEnd) {
				finish = true;
			} else if (intervalBegin > r) {
				high = mid - 1;
			} else if (intervalEnd <= r) {
				low = mid + 1;
			}
		}

		return mid;
	}

}
