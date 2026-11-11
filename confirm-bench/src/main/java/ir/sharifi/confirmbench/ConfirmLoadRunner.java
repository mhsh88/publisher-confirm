package ir.sharifi.confirmbench;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class ConfirmLoadRunner implements CommandLineRunner {

	private final RabbitTemplate rabbitTemplate;

	private final long timeoutMs;

	private final long processMs;

	private final long sendRate;

	private final int durationSeconds;

	private final AtomicLong arrived = new AtomicLong();

	private final AtomicLong sent = new AtomicLong();

	private final AtomicLong success = new AtomicLong();

	private final AtomicLong timeout = new AtomicLong();

	private final AtomicLong nack = new AtomicLong();

	private final Set<String> unsent = ConcurrentHashMap.newKeySet();

	public ConfirmLoadRunner(RabbitTemplate rabbitTemplate, @Value("${timeout-ms:1000}") long timeoutMs,
			@Value("${process-ms:5}") long processMs, @Value("${send-rate:1000}") long sendRate,
			@Value("${duration-seconds:60}") int durationSeconds) {
		this.rabbitTemplate = rabbitTemplate;
		this.timeoutMs = timeoutMs;
		this.processMs = processMs;
		this.sendRate = sendRate;
		this.durationSeconds = durationSeconds;
	}

	@Override
	public void run(String... args) {
		if (sendRate <= 0 || durationSeconds <= 0) {
			throw new IllegalArgumentException("send-rate and duration-seconds must be positive");
		}
		List<Row> rows = new ArrayList<>();
		long origin = System.nanoTime();
		long end = origin + TimeUnit.SECONDS.toNanos(durationSeconds);
		long nextArrival = origin;
		long nextReport = origin + TimeUnit.SECONDS.toNanos(1);
		int second = 0;
		long previousArrived = 0;
		long previousSent = 0;
		long previousSuccess = 0;
		long previousTimeout = 0;

		while (second < durationSeconds) {
			long now = System.nanoTime();
			while (now >= nextReport && second < durationSeconds) {
				second++;
				long[] mark = snapshot(rows, second, previousArrived, previousSent, previousSuccess, previousTimeout);
				previousArrived = mark[0];
				previousSent = mark[1];
				previousSuccess = mark[2];
				previousTimeout = mark[3];
				nextReport += TimeUnit.SECONDS.toNanos(1);
				now = System.nanoTime();
			}
			if (second >= durationSeconds) {
				break;
			}
			while (nextArrival <= now && nextArrival < end) {
				unsent.add(UUID.randomUUID().toString());
				arrived.incrementAndGet();
				nextArrival += TimeUnit.SECONDS.toNanos(1) / sendRate;
			}
			if (unsent.isEmpty()) {
				long wake = Math.min(nextArrival, nextReport);
				long wait = wake - System.nanoTime();
				if (wait > 0) {
					LockSupport.parkNanos(wait);
				}
				continue;
			}
			for (String id : List.copyOf(unsent)) {
				now = System.nanoTime();
				while (now >= nextReport && second < durationSeconds) {
					second++;
					long[] mark = snapshot(rows, second, previousArrived, previousSent, previousSuccess,
							previousTimeout);
					previousArrived = mark[0];
					previousSent = mark[1];
					previousSuccess = mark[2];
					previousTimeout = mark[3];
					nextReport += TimeUnit.SECONDS.toNanos(1);
					now = System.nanoTime();
				}
				if (second >= durationSeconds) {
					break;
				}
				while (nextArrival <= now && nextArrival < end) {
					unsent.add(UUID.randomUUID().toString());
					arrived.incrementAndGet();
					nextArrival += TimeUnit.SECONDS.toNanos(1) / sendRate;
				}
				if (unsent.contains(id)) {
					publish(id);
				}
			}
		}

		printTable(rows);
		int warmup = (int) Math.max(1, (timeoutMs + 999) / 1000);
		System.out.println(TimeoutShape.describe(rows, durationSeconds, warmup));
		System.out.println(summary(rows));
	}

	private String summary(List<Row> rows) {
		long arrivedSum = 0;
		long sentSum = 0;
		long successSum = 0;
		long timeoutSum = 0;
		int samples = 0;
		long finalUnsent = 0;
		for (Row row : rows) {
			if (row.second() < 1 || row.second() > durationSeconds) {
				continue;
			}
			arrivedSum += row.arrived();
			sentSum += row.sent();
			successSum += row.success();
			timeoutSum += row.timeout();
			finalUnsent = row.unsent();
			samples++;
		}
		long denom = Math.max(1, samples);
		return "mean/s arrived=" + (arrivedSum / denom) + " sent=" + (sentSum / denom) + " success="
				+ (successSum / denom) + " timeout=" + (timeoutSum / denom) + " final-unsent=" + finalUnsent
				+ " totals arrived=" + arrived.get() + " sent=" + sent.get() + " success=" + success.get()
				+ " timeout=" + timeout.get() + " nack=" + nack.get();
	}

	private long[] snapshot(List<Row> rows, int second, long previousArrived, long previousSent, long previousSuccess,
			long previousTimeout) {
		long arrivedNow = arrived.get();
		long sentNow = sent.get();
		long successNow = success.get();
		long timeoutNow = timeout.get();
		Row row = new Row(second, arrivedNow - previousArrived, sentNow - previousSent, successNow - previousSuccess,
				timeoutNow - previousTimeout, unsent.size());
		rows.add(row);
		System.out.printf("%d %d %d %d %d %d%n", row.second(), row.arrived(), row.sent(), row.success(), row.timeout(),
				row.unsent());
		System.out.flush();
		return new long[] { arrivedNow, sentNow, successNow, timeoutNow };
	}

	private void publish(String id) {
		CorrelationData correlationData = new CorrelationData(UUID.randomUUID().toString());
		sent.incrementAndGet();
		try {
			rabbitTemplate.convertAndSend(RabbitSetup.EXCHANGE, RabbitSetup.ROUTING_KEY, id, correlationData);
		}
		catch (RuntimeException ex) {
			sent.decrementAndGet();
			throw ex;
		}
		correlationData.getFuture().completable().orTimeout(timeoutMs, TimeUnit.MILLISECONDS).handle((confirm, error) -> {
			if (error != null) {
				Throwable cause = error instanceof java.util.concurrent.CompletionException && error.getCause() != null
						? error.getCause() : error;
				if (cause instanceof TimeoutException) {
					timeout.incrementAndGet();
					return null;
				}
				nack.incrementAndGet();
				return null;
			}
			if (confirm == null || !confirm.isAck() || correlationData.getReturned() != null) {
				nack.incrementAndGet();
				return null;
			}
			if (processMs > 0) {
				try {
					Thread.sleep(processMs);
				}
				catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
				}
			}
			if (unsent.remove(id)) {
				success.incrementAndGet();
			}
			return null;
		});
	}

	private static void printTable(List<Row> rows) {
		System.out.println();
		System.out.printf("%8s %8s %8s %8s %8s %8s%n", "second", "arrived", "sent", "success", "timeout", "unsent");
		for (Row row : rows) {
			System.out.printf("%8d %8d %8d %8d %8d %8d%n", row.second(), row.arrived(), row.sent(), row.success(),
					row.timeout(), row.unsent());
		}
	}

	public record Row(int second, long arrived, long sent, long success, long timeout, long unsent) {
	}
}
