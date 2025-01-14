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

package com.hw.lineage.common.plugin;

import static com.hw.lineage.common.util.Preconditions.checkNotNull;

/**
 * 插件接口。插件通常通过其 SPI（Service Provider Interface）扩展该接口，
 * 服务的具体实现类需遵循 SPI 合约并实现该接口。
 *
 * @description: Plugin（插件接口）
 * 提供了获取插件加载时所使用的类加载器的方法，便于某些需要动态类加载的插件在加载后继续使用。
 */
public interface Plugin {

    /**
     * 获取用于加载插件的类加载器的辅助方法。
     * 某些插件在加载后可能需要使用动态类加载，此方法提供了访问插件加载时类加载器的能力。
     *
     * @return 用于加载插件的类加载器
     */
    default ClassLoader getClassLoader() {
        return checkNotNull(
                this.getClass().getClassLoader(), // 获取当前插件类的类加载器
                "%s plugin with null class loader", // 异常消息模板
                this.getClass().getName() // 插件类的全限定名称
        );
    }
}

