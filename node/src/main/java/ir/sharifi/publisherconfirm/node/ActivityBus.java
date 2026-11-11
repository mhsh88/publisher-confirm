package ir.sharifi.publisherconfirm.node;

import java.util.Set;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.receptionist.Receptionist;
import org.apache.pekko.actor.typed.receptionist.ServiceKey;

public final class ActivityBus extends AbstractBehavior<ActivityBus.Command> {

	public static final ServiceKey<DashboardHub.Command> HUB_KEY = ServiceKey.create(DashboardHub.Command.class,
			"dashboard-hub");

	public sealed interface Command {
	}

	public record Publish(ActivityEvent event) implements Command {
	}

	private record Hubs(Receptionist.Listing listing) implements Command {
	}

	private Set<ActorRef<DashboardHub.Command>> hubs = Set.of();

	public static Behavior<Command> create() {
		return Behaviors.setup(ActivityBus::new);
	}

	private ActivityBus(ActorContext<Command> context) {
		super(context);
		ActorRef<Receptionist.Listing> adapter = context.messageAdapter(Receptionist.Listing.class, Hubs::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(HUB_KEY, adapter));
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(Publish.class, this::onPublish).onMessage(Hubs.class, this::onHubs)
				.build();
	}

	private Behavior<Command> onHubs(Hubs hubsMsg) {
		hubs = hubsMsg.listing().getServiceInstances(HUB_KEY);
		return this;
	}

	private Behavior<Command> onPublish(Publish publish) {
		for (ActorRef<DashboardHub.Command> hub : hubs) {
			hub.tell(new DashboardHub.Append(publish.event()));
		}
		return this;
	}
}
