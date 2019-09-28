package com.weibo.dip.portal.util;

/**
 * @author yurun
 * 
 * @datetime 2015-2-26 下午5:50:03
 */
public class Pair<F, S> {

	private F first;

	private S second;

	public Pair() {

	}

	public Pair(F first, S second) {
		this.first = first;

		this.second = second;
	}

	public F getFirst() {
		return first;
	}

	public void setFirst(F first) {
		this.first = first;
	}

	public S getSecond() {
		return second;
	}

	public void setSecond(S second) {
		this.second = second;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Pair)) {
			return false;
		}

		@SuppressWarnings("rawtypes")
		Pair pair = (Pair) obj;

		return first.equals(pair.first) && second.equals(pair.second);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * second.hashCode();
	}

	@Override
	public String toString() {
		return String.format("first: %s, second: %s", first, second);
	}

}
