package com.example.worktaskservice.infrastructure.graphql;

import com.example.worktaskservice.application.WorkTaskQueryService;
import com.example.worktaskservice.application.port.WorkTaskFilter;
import com.example.worktaskservice.application.port.WorkTaskPage;
import com.example.worktaskservice.domain.model.SubjectType;
import com.example.worktaskservice.domain.model.WorkTaskType;
import com.example.worktaskservice.infrastructure.graphql.dto.WorkTaskDto;
import com.example.worktaskservice.infrastructure.graphql.dto.WorkTaskFilterInput;
import com.example.worktaskservice.infrastructure.graphql.dto.WorkTaskPageDto;
import jakarta.inject.Inject;
import org.eclipse.microprofile.graphql.DefaultValue;
import org.eclipse.microprofile.graphql.Description;
import org.eclipse.microprofile.graphql.GraphQLApi;
import org.eclipse.microprofile.graphql.Name;
import org.eclipse.microprofile.graphql.Query;

import java.util.UUID;

@GraphQLApi
public class WorkTaskGraphQLApi {

    @Inject
    WorkTaskQueryService queryService;

    @Query("workTask")
    @Description("Look up a single WorkTask by id")
    public WorkTaskDto workTask(@Name("id") UUID id) {
        return queryService.findById(id).map(WorkTaskDto::from).orElse(null);
    }

    @Query("workTasks")
    @Description("List WorkTasks, optionally filtered and paged")
    public WorkTaskPageDto workTasks(
            @Name("filter") WorkTaskFilterInput filter,
            @Name("page") @DefaultValue("0") int page,
            @Name("size") @DefaultValue("20") int size) {
        WorkTaskPage result = queryService.search(toDomainFilter(filter), page, size);
        return new WorkTaskPageDto(
                result.items().stream().map(WorkTaskDto::from).toList(),
                result.totalCount(), page, size);
    }

    private WorkTaskFilter toDomainFilter(WorkTaskFilterInput filter) {
        if (filter == null) {
            return WorkTaskFilter.EMPTY;
        }
        return new WorkTaskFilter(
                filter.status(),
                filter.type() != null ? new WorkTaskType(filter.type()) : null,
                filter.subjectType() != null ? new SubjectType(filter.subjectType()) : null,
                filter.subjectId(),
                filter.assigneeId());
    }
}
