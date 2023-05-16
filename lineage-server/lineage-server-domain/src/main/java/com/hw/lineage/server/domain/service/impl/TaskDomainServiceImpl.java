/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hw.lineage.server.domain.service.impl;

import com.google.common.collect.ImmutableMap;
import com.hw.lineage.common.enums.SqlStatus;
import com.hw.lineage.common.enums.SqlType;
import com.hw.lineage.common.util.Base64Utils;
import com.hw.lineage.server.domain.entity.task.Task;
import com.hw.lineage.server.domain.entity.task.TaskSql;
import com.hw.lineage.server.domain.service.TaskDomainService;

import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hw.lineage.common.enums.SqlType.*;
import static com.hw.lineage.common.util.Preconditions.checkArgument;

/**
 * @description: TaskDomainServiceImpl
 * @author: HamaWhite
 */
@Service
public class TaskDomainServiceImpl implements TaskDomainService {

    private static final Map<String, SqlType> SQL_TYPE_MAP = ImmutableMap.<String, SqlType>builder()
            .put("SELECT", SELECT)
            .put("CREATE", CREATE)
            .put("DROP", DROP)
            .put("ALTER", ALTER)
            .put("INSERT", INSERT)
            .put("ANALYZE", ANALYZE)
            .put("DESCRIBE", DESCRIBE).put("DESC", DESCRIBE)
            .put("EXPLAIN", EXPLAIN)
            .put("USE", USE)
            .put("SHOW", SHOW)
            .put("LOAD", LOAD)
            .put("UNLOAD", UNLOAD)
            .put("SET", SET)
            .put("RESET", RESET)
            .put("ADD", JAR).put("REMOVE", JAR)
            .build();
    private static final List<SqlType> SUPPORT_SQL_TYPE =
            Arrays.asList(CREATE, DROP, ALTER, INSERT, USE, LOAD, UNLOAD, SET, RESET, JAR);

    @Override
    public void generateTaskSql(Task task) {
        task.getTaskSource().split()
                .forEach(e -> {
                    SqlType sqlType = extractSqlType(e.getSqlSource());
                    checkArgument(SUPPORT_SQL_TYPE.contains(sqlType),
                            "currently only supports SQL starting with %s, not yet: %s", SUPPORT_SQL_TYPE,
                            e.getSqlSource());
                    TaskSql taskSql = new TaskSql()
                            .setTaskId(task.getTaskId())
                            .setSqlSource(Base64Utils.encode(e.getSqlSource()))
                            .setSqlType(sqlType)
                            .setStartLineNumber(e.getStartLineNumber())
                            .setSqlStatus(SqlStatus.INIT)
                            .setInvalid(false);
                    task.addTaskSql(taskSql);
                });
    }

    private SqlType extractSqlType(String singleSql) {
        String sqlType = singleSql.split("\\s+")[0];
        return SQL_TYPE_MAP.get(sqlType.toUpperCase());
    }
}
