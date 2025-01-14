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
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * @description: FunctionInfo - 用于存储函数的元数据信息，包括函数名称、调用方式、实现类和描述等。
 */
@Data
@NoArgsConstructor
@Accessors(chain = true)
public class FunctionInfo {

    /**
     * 函数名称，用于标识函数。
     * 例如：`my_function`
     */
    private String functionName;

    /**
     * 函数的调用格式，描述函数的参数和使用方式。
     * 例如：`my_function(arg1, arg2)`
     */
    private String invocation;

    /**
     * 函数实现的完整类名，便于追溯到具体的实现类。
     * 例如：`com.example.MyFunction`
     */
    private String className;

    /**
     * 函数的描述信息，用于说明函数的功能和作用。
     * 例如：`This function performs XYZ operation.`
     */
    private String descr;
}

