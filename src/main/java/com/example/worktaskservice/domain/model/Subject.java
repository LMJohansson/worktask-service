package com.example.worktaskservice.domain.model;

import java.util.UUID;

public record Subject(SubjectType type, UUID id) {

    public Subject {
        if (type == null) throw new IllegalArgumentException("subject type must not be null");
        if (id == null) throw new IllegalArgumentException("subject id must not be null");
    }
}
