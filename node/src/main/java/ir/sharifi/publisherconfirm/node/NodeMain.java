package ir.sharifi.publisherconfirm.node;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.apache.pekko.actor.typed.ActorSystem;

public final class NodeMain {

	public static void main(String[] args) {
		String host = env("HOSTNAME", "127.0.0.1");
		String port = env("PEKKO_PORT", env("AKKA_PORT", "2551"));
		String seed = env("SEED_NODES", "pekko://ClusterSystem@" + host + ":" + port);
		int httpPort = Integer.parseInt(env("HTTP_PORT", "0"));

		String seedList = toHoconList(seed);
		Config config = ConfigFactory.parseString("pekko.remote.artery.canonical.hostname = \"" + host + "\"\n"
				+ "pekko.remote.artery.canonical.port = " + port + "\n" + "pekko.cluster.seed-nodes = " + seedList + "\n")
				.withFallback(ConfigFactory.load());

		ActorSystem.create(Guardian.create(RabbitSettings.fromEnv(), httpPort), "ClusterSystem", config);
	}

	private static String toHoconList(String seedCsv) {
		StringBuilder builder = new StringBuilder("[");
		String[] parts = seedCsv.split(",");
		for (int i = 0; i < parts.length; i++) {
			if (i > 0) {
				builder.append(", ");
			}
			builder.append('"').append(parts[i].trim()).append('"');
		}
		builder.append(']');
		return builder.toString();
	}

	private static String env(String key, String defaultValue) {
		String value = System.getenv(key);
		return value == null || value.isBlank() ? defaultValue : value;
	}

	private NodeMain() {
	}
}
