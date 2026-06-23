package com.example.worktaskservice.infrastructure.persistence;

import com.example.worktaskservice.application.port.WorkTaskFilter;
import com.example.worktaskservice.application.port.WorkTaskPage;
import com.example.worktaskservice.application.port.WorkTaskRepository;
import com.example.worktaskservice.domain.model.*;
import io.quarkus.hibernate.orm.panache.PanacheRepositoryBase;
import io.quarkus.panache.common.Page;
import jakarta.enterprise.context.ApplicationScoped;

import java.util.ArrayList;
import java.util.HashMap;
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
    public WorkTaskPage findAll(WorkTaskFilter filter, int page, int size) {
        var clauses = new ArrayList<String>();
        var params = new HashMap<String, Object>();
        if (filter.status() != null) {
            clauses.add("status = :status");
            params.put("status", filter.status());
        }
        if (filter.type() != null) {
            clauses.add("type = :type");
            params.put("type", filter.type().value());
        }
        if (filter.subjectType() != null) {
            clauses.add("subjectType = :subjectType");
            params.put("subjectType", filter.subjectType().value());
        }
        if (filter.subjectId() != null) {
            clauses.add("subjectId = :subjectId");
            params.put("subjectId", filter.subjectId());
        }
        if (filter.assigneeId() != null) {
            clauses.add("assigneeId = :assigneeId");
            params.put("assigneeId", filter.assigneeId());
        }

        var query = (clauses.isEmpty() ? findAll() : find(String.join(" and ", clauses), params))
                .page(Page.of(page, size));
        return new WorkTaskPage(query.list().stream().map(this::toDomain).toList(), query.count(), page, size);
    }

    @Override
    public void save(WorkTask task) {
        var entity = findByIdOptional(task.id()).orElseGet(WorkTaskEntity::new);
        entity.id          = task.id();
        entity.type        = task.type().value();
        entity.subjectType = task.subject().type().value();
        entity.subjectId   = task.subject().id();
        entity.source      = task.source().value();
        entity.title       = task.title();
        entity.description = task.description();
        entity.priority    = task.priority();
        entity.deadline    = task.deadline();
        entity.status      = task.status();
        entity.assigneeId  = task.assigneeId();
        entity.createdAt   = task.createdAt();
        entity.updatedAt   = task.updatedAt();
        var info = task.genericInfo();
        entity.genericInfoName            = info != null ? info.name() : null;
        entity.genericInfoType            = info != null ? info.type() : null;
        entity.genericInfoDatacontenttype = info != null ? info.datacontenttype() : null;
        entity.genericInfoDataschema      = info != null ? info.dataschema() : null;
        entity.genericInfoData            = info != null ? info.data() : null;
        persistAndFlush(entity);
    }

    private WorkTask toDomain(WorkTaskEntity e) {
        return WorkTask.reconstitute(
                e.id,
                new WorkTaskType(e.type),
                new Subject(new SubjectType(e.subjectType), e.subjectId),
                new Source(e.source),
                e.title, e.description, e.priority, e.deadline, toGenericInfo(e),
                e.status, e.assigneeId,
                e.createdAt, e.updatedAt);
    }

    private static GenericInfo toGenericInfo(WorkTaskEntity e) {
        if (e.genericInfoName == null) {
            return null;
        }
        return new GenericInfo(
                e.genericInfoName, e.genericInfoType, e.genericInfoDatacontenttype,
                e.genericInfoDataschema, e.genericInfoData);
    }
}
