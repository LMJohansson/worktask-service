package com.example.worktaskservice.application;

import com.example.worktaskservice.application.port.WorkTaskFilter;
import com.example.worktaskservice.application.port.WorkTaskPage;
import com.example.worktaskservice.application.port.WorkTaskRepository;
import com.example.worktaskservice.domain.model.WorkTask;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class WorkTaskQueryService {

    private final WorkTaskRepository repository;

    @Inject
    public WorkTaskQueryService(WorkTaskRepository repository) {
        this.repository = repository;
    }

    public Optional<WorkTask> findById(UUID id) {
        return repository.find(id);
    }

    public WorkTaskPage search(WorkTaskFilter filter, int page, int size) {
        return repository.findAll(filter, page, size);
    }
}
