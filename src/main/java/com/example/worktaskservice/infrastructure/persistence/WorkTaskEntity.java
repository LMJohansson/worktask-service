package com.example.worktaskservice.infrastructure.persistence;

import com.example.worktaskservice.domain.model.WorkTaskStatus;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "work_task")
public class WorkTaskEntity extends PanacheEntityBase {

    @Id
    @Column(columnDefinition = "uuid")
    public UUID id;

    @Column(nullable = false)
    public String type;

    @Column(name = "subject_type", nullable = false)
    public String subjectType;

    @Column(name = "subject_id", nullable = false, columnDefinition = "uuid")
    public UUID subjectId;

    @Column(nullable = false)
    public String title;

    public String description;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    public WorkTaskStatus status;

    @Column(name = "assignee_id", columnDefinition = "uuid")
    public UUID assigneeId;

    @Column(name = "created_at", nullable = false)
    public Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;
}
