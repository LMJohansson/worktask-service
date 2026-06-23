package com.example.worktaskservice.infrastructure.graphql.dto;

import com.example.worktaskservice.domain.model.GenericInfo;

import java.util.Base64;

/**
 * GraphQL view of a WorkTask's {@link GenericInfo}. The opaque {@code data} bytes are exposed
 * Base64-encoded, since GraphQL has no native binary type.
 */
public record GenericInfoDto(
        String name,
        String type,
        String datacontenttype,
        String dataschema,
        String dataBase64) {

    public static GenericInfoDto from(GenericInfo info) {
        if (info == null) {
            return null;
        }
        return new GenericInfoDto(
                info.name(),
                info.type(),
                info.datacontenttype(),
                info.dataschema(),
                Base64.getEncoder().encodeToString(info.data()));
    }
}
