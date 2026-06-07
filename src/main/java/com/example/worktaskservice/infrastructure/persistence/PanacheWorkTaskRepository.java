package com.example.worktaskservice.infrastructure.persistence;

import com.example.worktaskservice.application.port.WorkTaskRepository;
import com.example.worktaskservice.domain.model.*;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class PanacheWorkTaskRepository
        implements WorkTaskRepository, PanacheRepositoryBase<WorkTaskEntity, UUID> {

    @Override
    public Optional<WorkTask> find(UUID id) {
        return findByIdOptional(id).map(this::toDomain);
    }

    @Override
    public void save(WorkTask task) {
        var entity = findByIdOptional(task.id()).orElseGet(WorkTaskEntity::new);
        entity.id          = task.id();
        entity.type        = task.type().value();
        entity.subjectType = task.subject().type().value();
        entity.subjectId   = task.subject().id();
        entity.title       = task.title();
        entity.description = task.description();
        entity.priority    = task.priority();
        entity.deadline    = task.deadline();
        entity.status      = task.status();
        entity.assigneeId  = task.assigneeId();
        entity.createdAt   = task.createdAt();
        entity.updatedAt   = task.updatedAt();
        persistAndFlush(entity);
    }

    private WorkTask toDomain(WorkTaskEntity e) {
        return WorkTask.reconstitute(
                e.id,
                new WorkTaskType(e.type),
                new Subject(new SubjectType(e.subjectType), e.subjectId),
                e.title, e.description, e.priority, e.deadline, e.status, e.assigneeId,
                e.createdAt, e.updatedAt);
    }
}
