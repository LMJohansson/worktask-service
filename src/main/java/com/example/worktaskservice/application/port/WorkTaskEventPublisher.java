package com.example.worktaskservice.application.port;

import com.example.worktaskservice.domain.event.WorkTaskEvent;

public interface WorkTaskEventPublisher {
    void publish(WorkTaskEvent event);
}
