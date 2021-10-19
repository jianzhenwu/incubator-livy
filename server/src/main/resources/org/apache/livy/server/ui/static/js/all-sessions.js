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

function escapeHtml(unescapedText) {
  return $("<div>").text(unescapedText).html()
}

function loadSessionsTable() {
  $("#interactive-sessions-table").DataTable({
    autoWidth: true,
    paging: true,
    bLengthChange: true,
    bFilter: true,
    ordering: false,
    serverSide: true,
    ajax: function (data, callback, settings) {
      let params = {
        from: data.start,
        size: data.length,
        searchKey: data.search.value
      }
      $.ajax({
        type: "GET",
        url: "/sessions",
        cache: false,
        data: params,
        dataType: "json",
        success: function (res) {
          let returnData = {};
          returnData.recordsTotal = res.total;
          returnData.recordsFiltered = res.total;
          returnData.data = res.sessions;
          callback(returnData)
        }
      });
    },
    columns: [
      {
        name: "id", data: "id", render: function (data, type, row, meta) {
          return uiLink("session/" + row.id, row.id)
        }
      },
      {
        name: "appId", data: "appId", render: function (data, type, row, meta) {
          return appIdLink(row)
        }
      },
      {
        name: "name", data: "name", render: function (data, type, row, meta) {
          return escapeHtml(data)
        }
      },
      {name: "owner", data: "owner", defaultContent: "NULL"},
      {name: "proxyUser", data: "proxyUser", defaultContent: "NULL"},
      {name: "kind", data: "kind", defaultContent: "NULL"},
      {name: "state", data: "state"},
      {name: "server", data: "server", defaultContent: "NULL"},
      {
        name: "logs", data: "appInfo", render: function (data, type, row, meta) {
          if (row.state != null && row.state !== "") {
            return logLinks(row, "session")
          }
          return ""
        }, defaultContent: ""
      },
    ]
  });
}

function loadBatchesTable() {
  $("#batches-table").DataTable({
    autoWidth: true,
    paging: true,
    bLengthChange: true,
    bFilter: true,
    ordering: false,
    serverSide: true,
    ajax: function (data, callback, settings) {
      let params = {
        from: data.start,
        size: data.length,
        searchKey: data.search.value
      }
      $.ajax({
        type: "GET",
        url: "/batches",
        cache: false,
        data: params,
        dataType: "json",
        success: function (res) {
          let returnData = {};
          returnData.recordsTotal = res.total;
          returnData.recordsFiltered = res.total;
          returnData.data = res.sessions;
          callback(returnData)
        }
      });
    },
    columns: [
      {name: "id", data: "id"},
      {
        name: "appId", data: "appId", render: function (data, type, row, meta) {
          return appIdLink(row)
        }
      },
      {
        name: "name", data: "name", render: function (data, type, row, meta) {
          return escapeHtml(data)
        }
      },
      {name: "owner", data: "owner", defaultContent: "NULL"},
      {name: "proxyUser", data: "proxyUser", defaultContent: "NULL"},
      {name: "state", data: "state"},
      {name: "server", data: "server", defaultContent: "NULL"},
      {
        name: "logs", data: "appInfo", render: function (data, type, row, meta) {
          if (row.state != null && row.state !== "") {
            return logLinks(row, "batch")
          }
          return ""
        }, defaultContent: ""
      },
    ]
  });
}

$(document).ready(function () {
  $("#interactive-sessions").load(prependBasePath("/static/html/sessions-table.html .sessions-template"), function () {
    loadSessionsTable();
  })

  $("#batches").load(prependBasePath("/static/html/batches-table.html .sessions-template"), function (){
    loadBatchesTable();
  })
});
