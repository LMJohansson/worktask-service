package com.example.worktaskservice.infrastructure.kafka.serde;

import com.example.worktaskservice.commands.*;
import com.example.worktaskservice.events.*;
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
 * Registers the Avro schemas for the public topics into a Confluent Schema Registry: every message type
 * under its own subject (subject = Avro full name), plus an <b>Avro union of schema references</b> per
 * multi-type topic at {@code <topic>-value} — the {@code TopicNameStrategy} subject the SerDes resolve
 * with {@code use.latest.version}. The compact/state topic is single-type, so its value schema is
 * registered directly. Per-subject compatibility is set to match message direction: inbound commands
 * {@code BACKWARD}, outbound events + state {@code FORWARD}.
 *
 * <p>Talks to the Confluent <b>Schema Registry REST API</b> with {@code java.net.http} + Jackson, so it
 * behaves identically from a CDI startup bean (dev/test) and a plain {@code main} (CI/prod) — no
 * registry client to construct. Registration is idempotent (re-POSTing an identical schema returns the
 * existing id/version).
 *
 * @see SchemaRegistrarStartup dev/test bootstrap
 * @see SchemaRegistrarMain CI/prod entry point (Gradle {@code registerSchemas} task)
 */
public final class SchemaRegistrar {

    private static final Logger LOG = Logger.getLogger(SchemaRegistrar.class);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SR_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json";
    // Compatibility by message direction (see class doc).
    private static final String BACKWARD = "BACKWARD"; // inbound commands
    private static final String FORWARD = "FORWARD";   // outbound events + state

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

    private final String baseUrl; // bare registry base, e.g. http://localhost:8081
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    public SchemaRegistrar(String registryBaseUrl) {
        this.baseUrl = registryBaseUrl.endsWith("/")
                ? registryBaseUrl.substring(0, registryBaseUrl.length() - 1) : registryBaseUrl;
    }

    /** Block until the registry answers (it may still be starting under Dev Services). */
    public void awaitReady(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        var probe = HttpRequest.newBuilder(URI.create(baseUrl + "/subjects"))
                .timeout(Duration.ofSeconds(2)).GET().build();
        while (System.nanoTime() < deadline) {
            try {
                if (http.send(probe, HttpResponse.BodyHandlers.discarding()).statusCode() == 200) return;
            } catch (Exception ignored) {
                // registry not up yet
            }
            Thread.sleep(1000);
        }
        throw new IllegalStateException("Schema Registry not ready at " + baseUrl + " within " + timeout);
    }

    /** Register every member schema, then the per-topic union-of-references value schemas. */
    public void registerAll(String commandTopic, String deadLetterTopic, String eventTopic, String compactTopic) {
        Map<String, Integer> versions = new LinkedHashMap<>();
        for (Schema s : COMMAND_TYPES) versions.put(s.getFullName(), registerMember(s, BACKWARD));
        for (Schema s : EVENT_TYPES)   versions.put(s.getFullName(), registerMember(s, FORWARD));
        versions.put(STATE_TYPE.getFullName(), registerMember(STATE_TYPE, FORWARD));

        // Command + dead-letter topics carry command records; the event topic carries event records.
        registerUnion(commandTopic + "-value", COMMAND_TYPES, versions, BACKWARD);
        registerUnion(deadLetterTopic + "-value", COMMAND_TYPES, versions, BACKWARD);
        registerUnion(eventTopic + "-value", EVENT_TYPES, versions, FORWARD);
        // Compact/state topic is single-type — register the WorkTask schema directly.
        registerSchema(compactTopic + "-value", STATE_TYPE.toString(), List.of());
        setCompatibility(compactTopic + "-value", FORWARD);

        LOG.infof("Schema registration complete: %d member subjects + 4 topic-value subjects at %s",
                versions.size(), baseUrl);
    }

    private int registerMember(Schema schema, String compatibility) {
        int version = registerSchema(schema.getFullName(), schema.toString(), List.of());
        setCompatibility(schema.getFullName(), compatibility);
        return version;
    }

    private void registerUnion(String subject, List<Schema> members, Map<String, Integer> memberVersions,
                               String compatibility) {
        ArrayNode union = JSON.createArrayNode();         // ["<fullname1>", "<fullname2>", ...]
        List<Reference> refs = new ArrayList<>();
        for (Schema m : members) {
            union.add(m.getFullName());
            refs.add(new Reference(m.getFullName(), memberVersions.getOrDefault(m.getFullName(), 1)));
        }
        registerSchema(subject, union.toString(), refs);
        setCompatibility(subject, compatibility);
    }

    /** A Confluent schema reference: the referenced type's full name and the subject/version it lives at. */
    private record Reference(String fullName, int version) {}

    /** POST the schema under {@code subject} (idempotent); returns the resulting version number. */
    private int registerSchema(String subject, String schema, List<Reference> references) {
        try {
            ObjectNode body = JSON.createObjectNode();
            body.put("schemaType", "AVRO");
            body.put("schema", schema);
            ArrayNode refsNode = body.putArray("references");
            for (Reference r : references) {
                ObjectNode rn = refsNode.addObject();
                rn.put("name", r.fullName());     // the full name referenced inside the union schema
                rn.put("subject", r.fullName());  // the member subject (subject == full name)
                rn.put("version", r.version());
            }

            HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions"))
                    .header("Content-Type", SR_CONTENT_TYPE)
                    .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) {
                throw new IllegalStateException("Schema Registry register '" + subject + "' -> HTTP "
                        + resp.statusCode() + ": " + resp.body());
            }
            // POST returns only {"id":N}; fetch the version under this subject for use in union references.
            return latestVersion(subject);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to register '" + subject + "'", e);
        }
    }

    private int latestVersion(String subject) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/subjects/" + subject + "/versions/latest"))
                .timeout(Duration.ofSeconds(5)).GET().build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new IllegalStateException("Schema Registry latest-version '" + subject + "' -> HTTP "
                    + resp.statusCode() + ": " + resp.body());
        }
        return JSON.readTree(resp.body()).path("version").asInt();
    }

    /** Set the compatibility level for a subject (idempotent). */
    private void setCompatibility(String subject, String level) {
        try {
            String body = JSON.writeValueAsString(JSON.createObjectNode().put("compatibility", level));
            HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/config/" + subject))
                    .header("Content-Type", SR_CONTENT_TYPE)
                    .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() >= 300) {
                throw new IllegalStateException("Schema Registry set-compatibility '" + subject + "' -> HTTP "
                        + resp.statusCode() + ": " + resp.body());
            }
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set compatibility for '" + subject + "'", e);
        }
    }
}
