package com.example.worktaskservice.infrastructure.kafka.serde;

import com.example.worktaskservice.commands.*;
import com.example.worktaskservice.events.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.Schema;
import org.jboss.logging.Logger;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registers the Avro schemas for the public topics into Apicurio Registry: every message type as its
 * own artifact, plus an <b>Avro union of schema references</b> per multi-type topic at
 * {@code <topic>-value} — the {@code TopicIdStrategy} subject the SerDes resolve with {@code find-latest}.
 * The compact/state topic is single-type, so its value schema is registered directly.
 *
 * <p>Talks to the Apicurio <b>v3 REST API</b> with {@code java.net.http} + Jackson, so it behaves
 * identically from a CDI startup bean (dev/test) and a plain {@code main} (CI/prod) — no Kiota/Vert.x
 * client to construct. Registration is idempotent ({@code ifExists=FIND_OR_CREATE_VERSION}).
 *
 * @see SchemaRegistrarStartup dev/test bootstrap
 * @see SchemaRegistrarMain CI/prod entry point (Gradle {@code registerSchemas} task)
 */
public final class SchemaRegistrar {

    private static final Logger LOG = Logger.getLogger(SchemaRegistrar.class);
    // A dedicated, explicitly-named group (NOT "default"): Apicurio normalises the "default" group to a
    // null reference groupId, and the serde's reference resolver calls byGroupId(null) without remapping,
    // so union references to the default group fail to resolve. A real group name is preserved end-to-end.
    static final String GROUP = "worktask";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final List<Schema> COMMAND_TYPES = List.of(
            CreateWorkTask.getClassSchema(), AssignWorkTask.getClassSchema(), BeginWorkTask.getClassSchema(),
            PauseWorkTask.getClassSchema(), ResumeWorkTask.getClassSchema(), CompleteWorkTask.getClassSchema(),
            AbortWorkTask.getClassSchema(), CancelWorkTask.getClassSchema());

    private static final List<Schema> EVENT_TYPES = List.of(
            WorkTaskCreated.getClassSchema(), WorkTaskAssigned.getClassSchema(), WorkTaskReassigned.getClassSchema(),
            WorkTaskUnassigned.getClassSchema(), WorkTaskBegun.getClassSchema(), WorkTaskPaused.getClassSchema(),
            WorkTaskResumed.getClassSchema(), WorkTaskCompleted.getClassSchema(), WorkTaskAborted.getClassSchema(),
            WorkTaskCancelled.getClassSchema(), WorkTaskCommandRejected.getClassSchema());

    private static final Schema STATE_TYPE = com.example.worktaskservice.state.WorkTask.getClassSchema();

    private final String apiBase;      // <base>/apis/registry/v3
    private final String artifactsUrl; // <apiBase>/groups/worktask/artifacts
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public SchemaRegistrar(String registryBaseUrl) {
        String base = registryBaseUrl.endsWith("/")
                ? registryBaseUrl.substring(0, registryBaseUrl.length() - 1) : registryBaseUrl;
        this.apiBase = base + "/apis/registry/v3";
        this.artifactsUrl = apiBase + "/groups/" + GROUP + "/artifacts";
    }

    /** Block until the registry answers (it may still be starting under Dev Services for Compose). */
    public void awaitReady(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        var probe = HttpRequest.newBuilder(URI.create(apiBase + "/system/info"))
                .timeout(Duration.ofSeconds(2)).GET().build();
        while (System.nanoTime() < deadline) {
            try {
                if (http.send(probe, HttpResponse.BodyHandlers.discarding()).statusCode() == 200) return;
            } catch (Exception ignored) {
                // registry not up yet
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Apicurio registry not ready at " + apiBase + " within " + timeout);
    }

    /** Register every member schema, then the per-topic union-of-references value schemas. */
    public void registerAll(String commandTopic, String deadLetterTopic, String eventTopic, String compactTopic) {
        Map<String, String> versions = new LinkedHashMap<>();
        for (Schema s : COMMAND_TYPES) versions.put(s.getFullName(), registerMember(s));
        for (Schema s : EVENT_TYPES)   versions.put(s.getFullName(), registerMember(s));
        versions.put(STATE_TYPE.getFullName(), registerMember(STATE_TYPE));

        // Command + dead-letter topics carry command records; the event topic carries event records.
        registerUnion(commandTopic + "-value", COMMAND_TYPES, versions);
        registerUnion(deadLetterTopic + "-value", COMMAND_TYPES, versions);
        registerUnion(eventTopic + "-value", EVENT_TYPES, versions);
        // Compact/state topic is single-type — register the WorkTask schema directly.
        registerArtifact(compactTopic + "-value", STATE_TYPE.toString(), List.of());

        LOG.infof("Schema registration complete: %d member artifacts + 4 topic-value artifacts at %s",
                versions.size(), artifactsUrl);
    }

    private String registerMember(Schema schema) {
        return registerArtifact(schema.getFullName(), schema.toString(), List.of());
    }

    private void registerUnion(String artifactId, List<Schema> members, Map<String, String> memberVersions) {
        ArrayNode union = JSON.createArrayNode();         // ["<fullname1>", "<fullname2>", ...]
        List<Reference> refs = new ArrayList<>();
        for (Schema m : members) {
            union.add(m.getFullName());
            refs.add(new Reference(m.getFullName(), memberVersions.getOrDefault(m.getFullName(), "1")));
        }
        registerArtifact(artifactId, union.toString(), refs);
    }

    private record Reference(String fullName, String version) {}

    /** POST the artifact (idempotent); returns the resulting version string. */
    private String registerArtifact(String artifactId, String content, List<Reference> references) {
        try {
            ObjectNode body = JSON.createObjectNode();
            body.put("artifactId", artifactId);
            body.put("artifactType", "AVRO");
            ObjectNode contentNode = body.putObject("firstVersion").putObject("content");
            contentNode.put("content", content);
            contentNode.put("contentType", "application/json");
            ArrayNode refsNode = contentNode.putArray("references");
            for (Reference r : references) {
                ObjectNode rn = refsNode.addObject();
                rn.put("name", r.fullName());
                rn.put("groupId", GROUP);
                rn.put("artifactId", r.fullName());
                rn.put("version", r.version());
            }

            HttpRequest req = HttpRequest.newBuilder(URI.create(artifactsUrl + "?ifExists=FIND_OR_CREATE_VERSION"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) {
                throw new IllegalStateException("Apicurio register '" + artifactId + "' -> HTTP "
                        + resp.statusCode() + ": " + resp.body());
            }
            JsonNode version = JSON.readTree(resp.body()).path("version").path("version");
            return version.isMissingNode() || version.isNull() ? "1" : version.asText();
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register '" + artifactId + "'", e);
        }
    }
}
