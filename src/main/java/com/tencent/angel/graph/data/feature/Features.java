package com.tencent.angel.graph.data.feature;

public class Features<T> {
	private int[] featureSizes;
	private T[] featureValues;

	public Features(int[] featureSizes, T[] featureValues) {
		this.featureSizes = featureSizes;
		this.featureValues = featureValues;
	}

	public int[] getFeatureSizes() {
		return featureSizes;
	}

	public T[] getFeatureValues() {
		return featureValues;
	}
}
