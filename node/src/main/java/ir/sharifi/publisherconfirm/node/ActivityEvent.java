package ir.sharifi.publisherconfirm.node;

public record ActivityEvent(String kind, String workerId, String node, String otherWorkerId, String correlationId,
		String body, long at) implements CborSerializable {

	public static ActivityEvent sent(String workerId, String node, String toWorkerId, String correlationId,
			String body) {
		return new ActivityEvent("SENT", workerId, node, toWorkerId, correlationId, body, System.currentTimeMillis());
	}

	public static ActivityEvent confirmed(String workerId, String node, String toWorkerId, String correlationId,
			String status) {
		return new ActivityEvent("CONFIRM", workerId, node, toWorkerId, correlationId, status,
				System.currentTimeMillis());
	}

	public static ActivityEvent received(String workerId, String node, String fromWorkerId, String correlationId,
			String body) {
		return new ActivityEvent("RECEIVED", workerId, node, fromWorkerId, correlationId, body,
				System.currentTimeMillis());
	}

	public String toJson() {
		return "{\"kind\":\"" + esc(kind) + "\",\"workerId\":\"" + esc(workerId) + "\",\"node\":\"" + esc(node)
				+ "\",\"otherWorkerId\":\"" + esc(otherWorkerId) + "\",\"correlationId\":\"" + esc(correlationId)
				+ "\",\"body\":\"" + esc(body) + "\",\"at\":" + at + "}";
	}

	private static String esc(String value) {
		return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
