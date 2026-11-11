package ir.sharifi.publisherconfirm.node;

public record WireMessage(String fromWorkerId, String toWorkerId, String correlationId, String body)
		implements CborSerializable {

	public String toJson() {
		return "{\"fromWorkerId\":\"" + escape(fromWorkerId) + "\",\"toWorkerId\":\"" + escape(toWorkerId)
				+ "\",\"correlationId\":\"" + escape(correlationId) + "\",\"body\":\"" + escape(body) + "\"}";
	}

	public static WireMessage fromJson(String json) {
		return new WireMessage(field(json, "fromWorkerId"), field(json, "toWorkerId"), field(json, "correlationId"),
				field(json, "body"));
	}

	private static String field(String json, String name) {
		String key = "\"" + name + "\":\"";
		int start = json.indexOf(key);
		if (start < 0) {
			return "";
		}
		start += key.length();
		StringBuilder value = new StringBuilder();
		for (int i = start; i < json.length(); i++) {
			char c = json.charAt(i);
			if (c == '\\' && i + 1 < json.length()) {
				value.append(json.charAt(++i));
				continue;
			}
			if (c == '"') {
				break;
			}
			value.append(c);
		}
		return value.toString();
	}

	private static String escape(String value) {
		return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
