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

    @Column(name = "subject_id", nullable = false)
    public String subjectId;

    @Column(nullable = false)
    public String source;

    @Column(nullable = false)
    public String title;

    public String description;

    @Column(nullable = false)
    public int priority;

    public Instant deadline;

    // genericInfo — a CloudEvents-data-like result envelope; the whole group is null when absent,
    // otherwise every field is populated (see domain GenericInfo).
    @Column(name = "generic_info_name")
    public String genericInfoName;

    @Column(name = "generic_info_type")
    public String genericInfoType;

    @Column(name = "generic_info_datacontenttype")
    public String genericInfoDatacontenttype;

    @Column(name = "generic_info_dataschema")
    public String genericInfoDataschema;

    @Column(name = "generic_info_data")
    public byte[] genericInfoData;

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
