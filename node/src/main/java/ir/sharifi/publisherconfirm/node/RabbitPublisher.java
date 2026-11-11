package ir.sharifi.publisherconfirm.node;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Return;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.TimerScheduler;

public final class RabbitPublisher extends AbstractBehavior<RabbitPublisher.Command> {

	private static final Logger log = LoggerFactory.getLogger(RabbitPublisher.class);

	private static final Duration CONFIRM_TIMEOUT = Duration.ofSeconds(5);

	private static final String TIMER_KEY_PREFIX = "confirm-timeout-";

	public sealed interface Command {
	}

	public record Publish(String fromWorkerId, String toWorkerId, String body, ActorRef<SendResult> replyTo)
			implements Command {
	}

	private record ConfirmAck(long deliveryTag, boolean multiple) implements Command {
	}

	private record ConfirmNack(long deliveryTag, boolean multiple) implements Command {
	}

	private record ReturnedMsg(String correlationId, String routingKey, int replyCode, String replyText)
			implements Command {
	}

	private record ConfirmTimeout(long deliveryTag) implements Command {
	}

	private record Pending(String correlationId, ActorRef<SendResult> replyTo) {
	}

	private final TimerScheduler<Command> timers;

	private final Connection connection;

	private final Channel channel;

	private final Map<Long, Pending> pending = new HashMap<>();

	public static Behavior<Command> create(RabbitSettings settings) {
		return Behaviors.setup(context -> Behaviors.withTimers(timers -> {
			try {
				return new RabbitPublisher(context, timers, settings);
			}
			catch (Exception ex) {
				throw new IllegalStateException("failed to start RabbitPublisher", ex);
			}
		}));
	}

	private RabbitPublisher(ActorContext<Command> context, TimerScheduler<Command> timers, RabbitSettings settings)
			throws IOException, TimeoutException {
		super(context);
		this.timers = timers;
		this.connection = RabbitConnections.connect(settings,
				"cluster-publisher-" + context.getSystem().address().hostPort());
		this.channel = connection.createChannel();
		declareTopology(channel);
		channel.confirmSelect();
		ActorRef<Command> self = context.getSelf();
		channel.addConfirmListener((deliveryTag, multiple) -> self.tell(new ConfirmAck(deliveryTag, multiple)),
				(deliveryTag, multiple) -> self.tell(new ConfirmNack(deliveryTag, multiple)));
		channel.addReturnListener((Return returned) -> self.tell(new ReturnedMsg(
				returned.getProperties() != null ? returned.getProperties().getCorrelationId() : null,
				returned.getRoutingKey(), returned.getReplyCode(), returned.getReplyText())));
		log.info("RabbitPublisher ready");
	}

	static void declareTopology(Channel channel) throws IOException {
		channel.exchangeDeclare(MessagingConstants.EXCHANGE, "topic", true);
		channel.queueDeclare(MessagingConstants.QUEUE, true, false, false, null);
		channel.queueBind(MessagingConstants.QUEUE, MessagingConstants.EXCHANGE, MessagingConstants.ROUTING_KEY_PREFIX + "#");
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(Publish.class, this::onPublish).onMessage(ConfirmAck.class, this::onAck)
				.onMessage(ConfirmNack.class, this::onNack).onMessage(ReturnedMsg.class, this::onReturned)
				.onMessage(ConfirmTimeout.class, this::onTimeout).onSignal(PostStop.class, signal -> onStop()).build();
	}

	private Behavior<Command> onPublish(Publish command) throws IOException {
		String correlationId = UUID.randomUUID().toString();
		long deliveryTag = channel.getNextPublishSeqNo();
		pending.put(deliveryTag, new Pending(correlationId, command.replyTo()));
		timers.startSingleTimer(TIMER_KEY_PREFIX + deliveryTag, new ConfirmTimeout(deliveryTag), CONFIRM_TIMEOUT);
		WireMessage wire = new WireMessage(command.fromWorkerId(), command.toWorkerId(), correlationId, command.body());
		AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(correlationId).deliveryMode(2)
				.contentType("application/json")
				.build();
		String routingKey = MessagingConstants.routingKey(command.toWorkerId());
		log.info("PUBLISH id={} from={} to={} deliveryTag={} routingKey={} body={}", correlationId,
				command.fromWorkerId(), command.toWorkerId(), deliveryTag, routingKey, command.body());
		channel.basicPublish(MessagingConstants.EXCHANGE, routingKey, true, props,
				wire.toJson().getBytes(StandardCharsets.UTF_8));
		return this;
	}

	private Behavior<Command> onAck(ConfirmAck ack) {
		complete(ack.deliveryTag(), ack.multiple(), true, null);
		return this;
	}

	private Behavior<Command> onNack(ConfirmNack nack) {
		complete(nack.deliveryTag(), nack.multiple(), false, "broker nack");
		return this;
	}

	private Behavior<Command> onReturned(ReturnedMsg returned) {
		log.warn("RETURNED id={} routingKey={} replyCode={} replyText={}", returned.correlationId(),
				returned.routingKey(), returned.replyCode(), returned.replyText());
		return this;
	}

	private Behavior<Command> onTimeout(ConfirmTimeout timeout) {
		Pending entry = pending.remove(timeout.deliveryTag());
		if (entry != null) {
			log.error("CONFIRM wait failed id={} deliveryTag={}", entry.correlationId(), timeout.deliveryTag());
			entry.replyTo().tell(SendResult.timeout(entry.correlationId()));
		}
		return this;
	}

	private void complete(long deliveryTag, boolean multiple, boolean ack, String nackReason) {
		if (multiple) {
			Iterator<Map.Entry<Long, Pending>> iterator = pending.entrySet().iterator();
			while (iterator.hasNext()) {
				Map.Entry<Long, Pending> entry = iterator.next();
				if (entry.getKey() <= deliveryTag) {
					finish(entry.getKey(), entry.getValue(), ack, nackReason);
					iterator.remove();
				}
			}
			return;
		}
		Pending entry = pending.remove(deliveryTag);
		if (entry != null) {
			finish(deliveryTag, entry, ack, nackReason);
		}
	}

	private void finish(long deliveryTag, Pending entry, boolean ack, String nackReason) {
		timers.cancel(TIMER_KEY_PREFIX + deliveryTag);
		if (ack) {
			log.info("CONFIRM ack id={} deliveryTag={}", entry.correlationId(), deliveryTag);
			entry.replyTo().tell(SendResult.acked(entry.correlationId()));
		}
		else {
			log.error("CONFIRM nack id={} deliveryTag={} cause={}", entry.correlationId(), deliveryTag, nackReason);
			entry.replyTo().tell(SendResult.nacked(entry.correlationId(), nackReason));
		}
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
