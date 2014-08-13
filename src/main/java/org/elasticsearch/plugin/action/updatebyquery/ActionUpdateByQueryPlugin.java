/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugin.action.updatebyquery;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.updatebyquery.TransportShardUpdateByQueryAction;
import org.elasticsearch.action.updatebyquery.TransportUpdateByQueryAction;
import org.elasticsearch.action.updatebyquery.UpdateByQueryAction;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.rest.RestModule;
import org.elasticsearch.rest.action.RestActionModule;
import org.elasticsearch.rest.action.updatebyquery.RestUpdateByQueryAction;

/**
 * @author ofavre
 */
public class ActionUpdateByQueryPlugin extends AbstractPlugin {

    @Override public String name() {
        return "action-updatebyquery";
    }

    @Override public String description() {
        return "Update By Query action plugin";
    }

    @Override public void processModule(Module module) {
        if (module instanceof ActionModule) {
            ActionModule actionModule = (ActionModule) module;
            actionModule.registerAction(UpdateByQueryAction.INSTANCE, TransportUpdateByQueryAction.class,
                    TransportShardUpdateByQueryAction.class);
        } else if (module instanceof RestModule) {
            RestModule restModule = (RestModule) module;
            restModule.addRestAction(RestUpdateByQueryAction.class);
        }
    }

}
