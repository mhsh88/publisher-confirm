package ir.sharifi.publisherconfirm.node;

public final class MessagingConstants {

	public static final String EXCHANGE = "cluster.workers";

	public static final String QUEUE = "cluster.workers.inbox";

	public static final String ROUTING_KEY_PREFIX = "worker.";

	private MessagingConstants() {
	}

	public static String routingKey(String workerId) {
		return ROUTING_KEY_PREFIX + workerId;
	}
}
