package ir.sharifi.publisherconfirm.node;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.cluster.Member;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.typed.Cluster;

public final class DashboardHub extends AbstractBehavior<DashboardHub.Command> {

	private static final int MAX_EVENTS = 200;

	public sealed interface Command extends CborSerializable {
	}

	public record Append(ActivityEvent event) implements Command {
	}

	public record Subscribe(ActorRef<ClientMessage> client) implements Command {
	}

	public record Unsubscribe(ActorRef<ClientMessage> client) implements Command {
	}

	public record GetSnapshot(ActorRef<Snapshot> replyTo) implements Command {
	}

	public record ClientMessage(String json) implements CborSerializable {
	}

	public record Snapshot(String json) implements CborSerializable {
	}

	private final Deque<ActivityEvent> events = new ArrayDeque<>();

	private final Map<String, String> workers = new LinkedHashMap<>();

	private final List<ActorRef<ClientMessage>> clients = new ArrayList<>();

	public static Behavior<Command> create() {
		return Behaviors.setup(DashboardHub::new);
	}

	private DashboardHub(ActorContext<Command> context) {
		super(context);
		context.getSystem().receptionist().tell(Receptionist.register(ActivityBus.HUB_KEY, context.getSelf()));
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(Append.class, this::onAppend).onMessage(Subscribe.class, this::onSubscribe)
				.onMessage(Unsubscribe.class, this::onUnsubscribe).onMessage(GetSnapshot.class, this::onSnapshot)
				.build();
	}

	private Behavior<Command> onAppend(Append append) {
		ActivityEvent event = append.event();
		events.addFirst(event);
		while (events.size() > MAX_EVENTS) {
			events.removeLast();
		}
		if (event.workerId() != null && !event.workerId().isBlank()) {
			workers.put(event.workerId(), event.node());
		}
		String payload = "{\"type\":\"event\",\"event\":" + event.toJson() + "}";
		for (ActorRef<ClientMessage> client : List.copyOf(clients)) {
			client.tell(new ClientMessage(payload));
		}
		return this;
	}

	private Behavior<Command> onSubscribe(Subscribe subscribe) {
		clients.add(subscribe.client());
		subscribe.client().tell(new ClientMessage(snapshotJson("snapshot")));
		return this;
	}

	private Behavior<Command> onUnsubscribe(Unsubscribe unsubscribe) {
		clients.remove(unsubscribe.client());
		return this;
	}

	private Behavior<Command> onSnapshot(GetSnapshot getSnapshot) {
		getSnapshot.replyTo().tell(new Snapshot(snapshotJson("snapshot")));
		return this;
	}

	private String snapshotJson(String type) {
		Cluster cluster = Cluster.get(getContext().getSystem());
		String self = cluster.selfMember().address().toString();
		Set<String> nodes = new java.util.LinkedHashSet<>();
		for (Member member : cluster.state().getMembers()) {
			if (member.status().equals(MemberStatus.up())) {
				nodes.add(member.address().toString());
			}
		}
		String nodesJson = nodes.stream().map(n -> "\"" + esc(n) + "\"").collect(Collectors.joining(","));
		String workersJson = workers.entrySet().stream()
				.map(e -> "{\"id\":\"" + esc(e.getKey()) + "\",\"node\":\"" + esc(e.getValue()) + "\"}")
				.collect(Collectors.joining(","));
		String eventsJson = events.stream().map(ActivityEvent::toJson).collect(Collectors.joining(","));
		return "{\"type\":\"" + type + "\",\"self\":\"" + esc(self) + "\",\"nodes\":[" + nodesJson + "],\"workers\":["
				+ workersJson + "],\"events\":[" + eventsJson + "]}";
	}

	private static String esc(String value) {
		return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
	}
}
