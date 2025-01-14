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

package com.hw.lineage.common.model;

import lombok.Data;

import java.io.Serializable;

/**
 * @description: Property - 表示一个属性的元数据信息，包括名称、值、描述和是否为自定义属性的标识。
 */
@Data
public class Property implements Serializable {

    /**
     * 属性名称，用于标识该属性。
     * 例如：`maxRetries`
     */
    private String name;

    /**
     * 属性值，表示属性的具体值。
     * 例如：`5`
     */
    private String value;

    /**
     * 属性描述，用于说明属性的用途或含义。
     * 例如：`Maximum number of retry attempts.`
     */
    private String description;

    /**
     * 是否为自定义属性的标识。
     * 默认值为 `false`，表示为系统默认属性；
     * 若为 `true`，则表示该属性是用户自定义的。
     */
    private boolean custom = false;
}
