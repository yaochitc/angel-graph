package com.tencent.angel.graph.client;

public interface Sampler<T> {
	void init(T[] items, float[] weights);

	T sample();
}
