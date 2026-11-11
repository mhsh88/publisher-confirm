package ir.sharifi.publisherconfirm.node;

import static org.apache.pekko.http.javadsl.server.Directives.complete;
import static org.apache.pekko.http.javadsl.server.Directives.concat;
import static org.apache.pekko.http.javadsl.server.Directives.entity;
import static org.apache.pekko.http.javadsl.server.Directives.get;
import static org.apache.pekko.http.javadsl.server.Directives.getFromResource;
import static org.apache.pekko.http.javadsl.server.Directives.onComplete;
import static org.apache.pekko.http.javadsl.server.Directives.parameter;
import static org.apache.pekko.http.javadsl.server.Directives.path;
import static org.apache.pekko.http.javadsl.server.Directives.pathPrefix;
import static org.apache.pekko.http.javadsl.server.Directives.pathSingleSlash;
import static org.apache.pekko.http.javadsl.server.Directives.post;
import static org.apache.pekko.http.javadsl.unmarshalling.Unmarshaller.entityToString;

import java.time.Duration;
import java.util.concurrent.CompletionStage;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.Scheduler;
import org.apache.pekko.actor.typed.javadsl.AskPattern;
import org.apache.pekko.cluster.sharding.typed.javadsl.ClusterSharding;
import org.apache.pekko.cluster.sharding.typed.javadsl.EntityRef;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.http.javadsl.model.ContentTypes;
import org.apache.pekko.http.javadsl.model.HttpEntities;
import org.apache.pekko.http.javadsl.model.StatusCodes;
import org.apache.pekko.http.javadsl.server.Route;

public final class HttpServer {

	private static final Duration ASK_TIMEOUT = Duration.ofSeconds(8);

	private final ActorSystem<?> system;

	private final ClusterSharding sharding;

	private final ActorRef<DashboardHub.Command> dashboardHub;

	private final Scheduler scheduler;

	public HttpServer(ActorSystem<?> system, ClusterSharding sharding, ActorRef<DashboardHub.Command> dashboardHub) {
		this.system = system;
		this.sharding = sharding;
		this.dashboardHub = dashboardHub;
		this.scheduler = system.scheduler();
	}

	public CompletionStage<ServerBinding> bind(String host, int port) {
		return Http.get(system).newServerAt(host, port).bind(routes());
	}

	private Route routes() {
		return concat(pathSingleSlash(() -> get(() -> getFromResource("web/index.html"))),
				path("app.js", () -> get(() -> getFromResource("web/app.js"))),
				path("styles.css", () -> get(() -> getFromResource("web/styles.css"))),
				pathPrefix("api",
						() -> concat(path("snapshot", () -> get(this::snapshot)),
								path("send",
										() -> post(() -> parameter("from",
												from -> parameter("to", to -> entity(entityToString(),
														body -> completeSend(from, to, body)))))))));
	}

	private Route snapshot() {
		CompletionStage<DashboardHub.Snapshot> snap = AskPattern.ask(dashboardHub, DashboardHub.GetSnapshot::new,
				ASK_TIMEOUT, scheduler);
		return onComplete(snap, attempt -> {
			if (attempt.isSuccess()) {
				return complete(StatusCodes.OK,
						HttpEntities.create(ContentTypes.APPLICATION_JSON, attempt.get().json()));
			}
			return complete(StatusCodes.SERVICE_UNAVAILABLE);
		});
	}

	private Route completeSend(String from, String to, String body) {
		String payload = body == null || body.isBlank() ? "ping" : body;
		EntityRef<Worker.Command> worker = sharding.entityRefFor(Worker.TYPE_KEY, from);
		CompletionStage<SendResult> result = AskPattern.ask(worker,
				(ActorRef<SendResult> replyTo) -> new Worker.Send(to, payload, replyTo), ASK_TIMEOUT, scheduler);
		return onComplete(result, attempt -> {
			if (attempt.isSuccess()) {
				return complete(StatusCodes.OK,
						HttpEntities.create(ContentTypes.APPLICATION_JSON, attempt.get().toJson()));
			}
			return complete(StatusCodes.GATEWAY_TIMEOUT,
					HttpEntities.create(ContentTypes.APPLICATION_JSON, SendResult.timeout("unknown").toJson()));
		});
	}
}
