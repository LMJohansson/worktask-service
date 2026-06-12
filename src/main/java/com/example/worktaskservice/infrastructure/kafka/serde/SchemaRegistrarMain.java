package com.example.worktaskservice.infrastructure.kafka.serde;

/**
 * CI/prod entry point for schema registration, invoked by the Gradle {@code registerSchemas} task.
 * Registers member + union-of-references schemas against the registry given by
 * {@code -Dapicurio.registry.url=…} (or {@code SCHEMA_REGISTRY_URL}). Topic names default to the
 * {@code application.properties} values and can be overridden with {@code -Dworktask.topics.*}.
 */
public final class SchemaRegistrarMain {

    private SchemaRegistrarMain() {}

    public static void main(String[] args) {
        String url = prop("apicurio.registry.url",
                System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081"));
        String command    = prop("worktask.topics.command",     "work.tasks.worktask.public.worktask.command");
        String event      = prop("worktask.topics.event",       "work.tasks.worktask.public.worktask.event");
        String compact    = prop("worktask.topics.compact",     "work.tasks.worktask.public.worktask.compact");
        String deadLetter = prop("worktask.topics.dead-letter", "work.tasks.worktask.private.worktask.dead-letter");

        new SchemaRegistrar(url).registerAll(command, deadLetter, event, compact);
        System.out.println("Registered WorkTask schemas to " + url);
    }

    private static String prop(String key, String fallback) {
        return System.getProperty(key, fallback);
    }
}
