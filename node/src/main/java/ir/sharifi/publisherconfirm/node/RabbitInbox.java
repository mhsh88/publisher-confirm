package ir.sharifi.publisherconfirm.node;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;

public final class RabbitInbox extends AbstractBehavior<RabbitInbox.Command> {

	private static final Logger log = LoggerFactory.getLogger(RabbitInbox.class);

	public sealed interface Command {
	}

	private record Deliver(WireMessage message) implements Command {
	}

	private final Connection connection;

	private final Channel channel;

	private final ClusterSharding sharding;

	public static Behavior<Command> create(RabbitSettings settings, ClusterSharding sharding) {
		return Behaviors.setup(context -> {
			try {
				return new RabbitInbox(context, settings, sharding);
			}
			catch (Exception ex) {
				throw new IllegalStateException("failed to start RabbitInbox", ex);
			}
		});
	}

	private RabbitInbox(ActorContext<Command> context, RabbitSettings settings, ClusterSharding sharding)
			throws IOException, TimeoutException {
		super(context);
		this.sharding = sharding;
		this.connection = RabbitConnections.connect(settings,
				"cluster-inbox-" + context.getSystem().address().hostPort());
		this.channel = connection.createChannel();
		RabbitPublisher.declareTopology(channel);
		ActorRef<Command> self = context.getSelf();
		DeliverCallback callback = (consumerTag, delivery) -> {
			String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
			WireMessage message = WireMessage.fromJson(json);
			self.tell(new Deliver(message));
		};
		channel.basicConsume(MessagingConstants.QUEUE, true, callback, consumerTag -> {
		});
		log.info("RabbitInbox consuming queue={}", MessagingConstants.QUEUE);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(Deliver.class, this::onDeliver).onSignal(PostStop.class, signal -> onStop())
				.build();
	}

	private Behavior<Command> onDeliver(Deliver deliver) {
		WireMessage message = deliver.message();
		log.info("INBOX forward to worker={} from={} correlationId={}", message.toWorkerId(), message.fromWorkerId(),
				message.correlationId());
		EntityRef<Worker.Command> target = sharding.entityRefFor(Worker.TYPE_KEY, message.toWorkerId());
		target.tell(new Worker.Incoming(message.fromWorkerId(), message.body(), message.correlationId()));
		return this;
	}

	private Behavior<Command> onStop() {
		try {
			if (channel != null && channel.isOpen()) {
				channel.close();
			}
		}
		catch (Exception ignored) {
		}
		try {
			if (connection != null && connection.isOpen()) {
				connection.close();
			}
		}
		catch (Exception ignored) {
		}
		return Behaviors.stopped();
	}
}
