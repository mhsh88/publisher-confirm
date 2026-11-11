package ir.sharifi.publisherconfirm.node;

public record RabbitSettings(String host, int port, String username, String password) implements CborSerializable {

	public static RabbitSettings fromEnv() {
		return new RabbitSettings(env("RABBITMQ_HOST", "localhost"),
				Integer.parseInt(env("RABBITMQ_PORT", "5672")), env("RABBITMQ_USER", "guest"),
				env("RABBITMQ_PASSWORD", "guest"));
	}

	private static String env(String key, String defaultValue) {
		String value = System.getenv(key);
		return value == null || value.isBlank() ? defaultValue : value;
	}
}
