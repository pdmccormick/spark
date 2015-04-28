/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function(globals, $) {
    /* At this point, the page is not yet ready. */
    var G = {
        basePath: "/",
    };

    var STAGE_KILL_URI = "stages/stage/kill/";

    function handleStageKill(ev) {
        ev.preventDefault();

        var elem = $(ev.target);
        var stage_id = elem.data('stage-id');

        if (window.confirm("Are you sure you want to kill stage " + stage_id + " ?")) {
            var form = $(
                "<form action='" + G.basePath + STAGE_KILL_URI + "' method='POST'>" +
                    "<input type='hidden' name='id' value='" + stage_id + "'>" +
                    "<input type='hidden' name='terminate' value='true'>" +
                "</form>");
            form.submit();
        }
    }

    $(function() {
        /* At this point, the page _is_ ready. */

        $(".action-stage-kill").click(handleStageKill);
    });

    globals.SPARK_DRIVER_WEBUI = {
        setBasePath: function(basePath) {
            G.basePath = basePath;
        }
    };
})(window, jQuery);

