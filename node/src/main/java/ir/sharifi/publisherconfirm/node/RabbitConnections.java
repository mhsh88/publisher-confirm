package ir.sharifi.publisherconfirm.node;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RabbitConnections {

	private static final Logger log = LoggerFactory.getLogger(RabbitConnections.class);

	private RabbitConnections() {
	}

	public static Connection connect(RabbitSettings settings, String name) throws IOException, TimeoutException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(settings.host());
		factory.setPort(settings.port());
		factory.setUsername(settings.username());
		factory.setPassword(settings.password());
		factory.setAutomaticRecoveryEnabled(true);
		Exception last = null;
		for (int attempt = 1; attempt <= 30; attempt++) {
			try {
				Connection connection = factory.newConnection(name);
				log.info("Rabbit connected name={} attempt={}", name, attempt);
				return connection;
			}
			catch (Exception ex) {
				last = ex;
				log.warn("Rabbit connect failed name={} attempt={}: {}", name, attempt, ex.toString());
				try {
					Thread.sleep(1000L);
				}
				catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					throw new IOException("interrupted while connecting to Rabbit", interrupted);
				}
			}
		}
		if (last instanceof IOException ioException) {
			throw ioException;
		}
		if (last instanceof TimeoutException timeoutException) {
			throw timeoutException;
		}
		throw new IOException("unable to connect to Rabbit", last);
	}
}
