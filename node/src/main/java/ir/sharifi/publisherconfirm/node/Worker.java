package ir.sharifi.publisherconfirm.node;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityTypeKey;

public final class Worker extends AbstractBehavior<Worker.Command> {

	private static final Logger log = LoggerFactory.getLogger(Worker.class);

	public static final EntityTypeKey<Command> TYPE_KEY = EntityTypeKey.create(Command.class, "Worker");

	private static final Duration PUBLISH_ASK_TIMEOUT = Duration.ofSeconds(6);

	public sealed interface Command extends CborSerializable {
	}

	public record Send(String toWorkerId, String body, ActorRef<SendResult> replyTo) implements Command {
	}

	public record Incoming(String fromWorkerId, String body, String correlationId) implements Command {
	}

	private record PublishDone(SendResult result, String toWorkerId, ActorRef<SendResult> replyTo) implements Command {
	}

	private final String workerId;

	private final String nodeAddress;

	private final ActorRef<RabbitPublisher.Command> rabbitPublisher;

	private final ActorRef<ActivityBus.Command> activityBus;

	public static Behavior<Command> create(String workerId, ActorRef<RabbitPublisher.Command> rabbitPublisher,
			ActorRef<ActivityBus.Command> activityBus) {
		return Behaviors.setup(context -> new Worker(context, workerId, rabbitPublisher, activityBus));
	}

	private Worker(ActorContext<Command> context, String workerId, ActorRef<RabbitPublisher.Command> rabbitPublisher,
			ActorRef<ActivityBus.Command> activityBus) {
		super(context);
		this.workerId = workerId;
		this.nodeAddress = context.getSystem().address().toString();
		this.rabbitPublisher = rabbitPublisher;
		this.activityBus = activityBus;
		log.info("Worker entity started id={} on {}", workerId, nodeAddress);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(Send.class, this::onSend).onMessage(Incoming.class, this::onIncoming)
				.onMessage(PublishDone.class, this::onPublishDone).build();
	}

	private Behavior<Command> onSend(Send command) {
		log.info("Worker {} sending to {} body={}", workerId, command.toWorkerId(), command.body());
		activityBus.tell(new ActivityBus.Publish(
				ActivityEvent.sent(workerId, nodeAddress, command.toWorkerId(), "pending", command.body())));
		CompletionStage<SendResult> result = AskPattern.ask(rabbitPublisher,
				(ActorRef<SendResult> replyTo) -> new RabbitPublisher.Publish(workerId, command.toWorkerId(),
						command.body(), replyTo),
				PUBLISH_ASK_TIMEOUT, getContext().getSystem().scheduler());
		getContext().pipeToSelf(result, (ok, ex) -> {
			if (ex != null) {
				return new PublishDone(SendResult.timeout("unknown"), command.toWorkerId(), command.replyTo());
			}
			return new PublishDone(ok, command.toWorkerId(), command.replyTo());
		});
		return this;
	}

	private Behavior<Command> onPublishDone(PublishDone done) {
		activityBus.tell(new ActivityBus.Publish(ActivityEvent.confirmed(workerId, nodeAddress, done.toWorkerId(),
				done.result().correlationId(), done.result().status())));
		done.replyTo().tell(done.result());
		return this;
	}

	private Behavior<Command> onIncoming(Incoming incoming) {
		log.info("RECEIVED via Rabbit worker={} from={} correlationId={} body={}", workerId, incoming.fromWorkerId(),
				incoming.correlationId(), incoming.body());
		activityBus.tell(new ActivityBus.Publish(ActivityEvent.received(workerId, nodeAddress, incoming.fromWorkerId(),
				incoming.correlationId(), incoming.body())));
		return this;
	}
}
