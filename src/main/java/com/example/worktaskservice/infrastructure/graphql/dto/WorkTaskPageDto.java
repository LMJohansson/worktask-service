package com.example.worktaskservice.infrastructure.graphql.dto;

import java.util.List;

public record WorkTaskPageDto(List<WorkTaskDto> items, long totalCount, int page, int size) {
}
