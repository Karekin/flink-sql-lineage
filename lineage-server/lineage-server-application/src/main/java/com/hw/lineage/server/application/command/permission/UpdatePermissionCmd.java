package com.hw.lineage.server.application.command.permission;

import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.io.Serializable;

/**
 * @description: UpdatePermissionCmd
 * @author: HamaWhite
 * @version: 1.0.0
 */
@Data
public class UpdatePermissionCmd implements Serializable {

    @ApiModelProperty(hidden = true)
    private Long permissionId;

    private String permissionGroup;

    private String permissionName;

    private String permissionCode;
}
