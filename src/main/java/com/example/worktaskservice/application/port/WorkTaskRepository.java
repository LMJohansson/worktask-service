package com.example.worktaskservice.application.port;

import com.example.worktaskservice.domain.model.WorkTask;

import java.util.Optional;
import java.util.UUID;

public interface WorkTaskRepository {
    Optional<WorkTask> find(UUID id);
    void save(WorkTask workTask);
}
