package ir.sharifi.publisherconfirm.node;

public record SendResult(String correlationId, String status, String detail) implements CborSerializable {

	public static SendResult acked(String id) {
		return new SendResult(id, "ACKED", "broker confirmed publish");
	}

	public static SendResult nacked(String id, String reason) {
		return new SendResult(id, "NACKED", reason);
	}

	public static SendResult timeout(String id) {
		return new SendResult(id, "TIMEOUT", "no confirm within timeout");
	}

	public String toJson() {
		return "{\"correlationId\":\"" + escape(correlationId) + "\",\"status\":\"" + escape(status)
				+ "\",\"detail\":\"" + escape(detail) + "\"}";
	}

	private static String escape(String value) {
		return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
