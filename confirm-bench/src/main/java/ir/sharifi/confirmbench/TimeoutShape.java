package ir.sharifi.confirmbench;

import java.util.ArrayList;
import java.util.List;

final class TimeoutShape {

	private TimeoutShape() {
	}

	static String describe(List<ConfirmLoadRunner.Row> rows, int durationSeconds, int warmupSeconds) {
		List<Long> series = new ArrayList<>();
		for (ConfirmLoadRunner.Row row : rows) {
			if (row.second() > warmupSeconds && row.second() <= durationSeconds) {
				series.add(row.timeout());
			}
		}
		if (series.size() < 4) {
			return "timeouts per second: flattened";
		}
		double mean = series.stream().mapToLong(Long::longValue).average().orElse(0);
		long min = series.stream().mapToLong(Long::longValue).min().orElse(0);
		long max = series.stream().mapToLong(Long::longValue).max().orElse(0);
		double slope = slope(series);
		int reversals = reversals(series, Math.max(5d, mean * 0.05d));
		String shape;
		if (reversals >= 4 && (max - min) >= Math.max(20d, mean * 0.25d)) {
			shape = "oscillated";
		}
		else if (slope >= 1d && series.get(series.size() - 1) > series.get(0) + 10) {
			shape = "climbed";
		}
		else {
			shape = "flattened";
		}
		return "timeouts per second: " + shape + " (mean=" + Math.round(mean) + " min=" + min + " max=" + max
				+ " slope=" + String.format("%.2f", slope) + "/s reversals=" + reversals + ")";
	}

	private static double slope(List<Long> series) {
		int n = series.size();
		double sumX = 0;
		double sumY = 0;
		double sumXY = 0;
		double sumXX = 0;
		for (int i = 0; i < n; i++) {
			sumX += i;
			sumY += series.get(i);
			sumXY += (double) i * series.get(i);
			sumXX += (double) i * i;
		}
		double denom = n * sumXX - sumX * sumX;
		if (denom == 0) {
			return 0;
		}
		return (n * sumXY - sumX * sumY) / denom;
	}

	private static int reversals(List<Long> series, double noise) {
		int sign = 0;
		int changes = 0;
		for (int i = 1; i < series.size(); i++) {
			double delta = series.get(i) - series.get(i - 1);
			if (Math.abs(delta) < noise) {
				continue;
			}
			int next = delta > 0 ? 1 : -1;
			if (sign != 0 && next != sign) {
				changes++;
			}
			sign = next;
		}
		return changes;
	}
}
