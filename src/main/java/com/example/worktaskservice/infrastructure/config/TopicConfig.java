package com.example.worktaskservice.infrastructure.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class TopicConfig {

    @ConfigProperty(name = "worktask.topics.command",
            defaultValue = "work.tasks.worktask.public.worktask.command")
    String commandTopic;

    @ConfigProperty(name = "worktask.topics.event",
            defaultValue = "work.tasks.worktask.public.worktask.event")
    String eventTopic;

    @ConfigProperty(name = "worktask.topics.compact",
            defaultValue = "work.tasks.worktask.public.worktask.compact")
    String compactTopic;

    @ConfigProperty(name = "worktask.topics.dead-letter",
            defaultValue = "work.tasks.worktask.private.worktask.dead-letter")
    String deadLetterTopic;

    public String commandTopic() { return commandTopic; }
    public String eventTopic() { return eventTopic; }
    public String compactTopic() { return compactTopic; }
    public String deadLetterTopic() { return deadLetterTopic; }
}
