package com.example.worktaskservice.application.port;

import com.example.worktaskservice.domain.model.WorkTask;

import java.util.List;

public record WorkTaskPage(List<WorkTask> items, long totalCount, int page, int size) {
}
