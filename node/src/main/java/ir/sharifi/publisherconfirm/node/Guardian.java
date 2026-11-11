package ir.sharifi.publisherconfirm.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.cluster.ClusterEvent;
import org.apache.pekko.cluster.typed.Cluster;
import org.apache.pekko.cluster.typed.Subscribe;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.Entity;

public final class Guardian {

	private static final Logger log = LoggerFactory.getLogger(Guardian.class);

	private Guardian() {
	}

	public static Behavior<ClusterEvent.ClusterDomainEvent> create(RabbitSettings rabbitSettings, int httpPort) {
		return Behaviors.setup(context -> {
			ActorRef<ActivityBus.Command> activityBus = context.spawn(ActivityBus.create(), "activity-bus");
			ActorRef<RabbitPublisher.Command> rabbitPublisher = context.spawn(RabbitPublisher.create(rabbitSettings),
					"rabbit-publisher");

			ClusterSharding sharding = ClusterSharding.get(context.getSystem());
			sharding.init(Entity.of(Worker.TYPE_KEY,
					entityContext -> Worker.create(entityContext.getEntityId(), rabbitPublisher, activityBus)));

			context.spawn(RabbitInbox.create(rabbitSettings, sharding), "rabbit-inbox");

			Cluster cluster = Cluster.get(context.getSystem());
			cluster.subscriptions().tell(Subscribe.create(context.getSelf(), ClusterEvent.ClusterDomainEvent.class));

			if (httpPort > 0) {
				ActorRef<DashboardHub.Command> dashboardHub = context.spawn(DashboardHub.create(), "dashboard-hub");
				new HttpServer(context.getSystem(), sharding, dashboardHub).bind("0.0.0.0", httpPort)
						.whenComplete((binding, error) -> {
							if (error != null) {
								log.error("HTTP bind failed", error);
								context.getSystem().terminate();
								return;
							}
							log.info("HTTP listening on {}", binding.localAddress());
							System.out.println("HTTP listening on " + binding.localAddress());
						});
			}

			log.info("Node guardian started address={}", context.getSystem().address());
			return Behaviors.receive(ClusterEvent.ClusterDomainEvent.class)
					.onMessage(ClusterEvent.MemberUp.class, event -> {
						log.info("Cluster member up: {}", event.member());
						return Behaviors.same();
					})
					.onMessage(ClusterEvent.MemberRemoved.class, event -> {
						log.info("Cluster member removed: {}", event.member());
						return Behaviors.same();
					})
					.onAnyMessage(event -> {
						log.debug("Cluster event: {}", event);
						return Behaviors.same();
					})
					.build();
		});
	}
}
