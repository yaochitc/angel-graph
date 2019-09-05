package com.tencent.angel.graph.client;

import java.util.ArrayList;
import java.util.List;

public class FastSampler<T> implements Sampler<T> {
	private List<T> itemList;
	private AliasMethod aliasMethod;

	@Override
	public void init(T[] items, float[] weights) {
		assert items.length == weights.length;

		int size = items.length;
		itemList = new ArrayList<>(size);
		float sumWeight = 0f;
		for (int i = 0; i < size; i++) {
			itemList.add(items[i]);
			sumWeight += weights[i];
		}

		float[] normWeights = new float[size];
		for (int i = 0; i < size; i++) {
			normWeights[i] = weights[i] / sumWeight;
		}
		aliasMethod = new AliasMethod(normWeights);
	}

	@Override
	public T sample() {
		int column = aliasMethod.next();
		return itemList.get(column);
	}

	static class AliasMethod {
		private final Float[] probs;
		private final Integer[] alias;

		AliasMethod(float[] weights) {
			int size = weights.length;
			probs = new Float[size];
			alias = new Integer[size];

			List<Integer> large = new ArrayList<>();
			List<Integer> small = new ArrayList<>();
			float avg = 1.0f / size;
			for (int i = 0; i < size; i++) {
				if (weights[i] > avg) {
					large.add(i);
				} else {
					small.add(i);
				}
			}

			while (large.size() > 0 && small.size() > 0) {
				int less = small.remove(small.size() - 1);
				int more = large.remove(large.size() - 1);
				probs[less] = weights[less] * size;
				alias[less] = more;
				weights[more] = weights[more] + weights[less] - avg;
				if (weights[more] > avg) {
					large.add(more);
				} else {
					small.add(more);
				}
			}

			while (small.size() > 0) {
				int less = small.remove(small.size() - 1);
				probs[less] = 1.0f;
			}

			while (large.size() > 0) {
				int more = large.remove(large.size() - 1);
				probs[more] = 1.0f;
			}
		}

		int next() {
			int column = (int) Math.floor(Math.random() * probs.length);
			boolean coinToss = Math.random() < probs[column];
			return coinToss ? column : alias[column];
		}
	}
}
