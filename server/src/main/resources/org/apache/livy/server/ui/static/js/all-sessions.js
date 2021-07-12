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
      {data: "id", render: function (data, type, row, meta) {return uiLink("session/" + row.id, row.id)}},
      {data: "id", render: function (data, type, row, meta) { return appIdLink(row)}},
      {data: "name", render: function (data, type, row, meta) { return escapeHtml(data)}},
      {data: "owner", "defaultContent": "NULL"},
      {data: "proxyUser", "defaultContent": "NULL"},
      {data: "kind", "defaultContent": "NULL"},
      {data: "state"},
      {data: "id", render: function (data, type, row, meta) {return logLinks(row, "session")}},
      {data: "server", "defaultContent": "NULL"},
    ]
  });
}

function loadBatchesTable() {
  $("#batches-table").DataTable({
    autoWidth: true,
    paging: true,
    bLengthChange: true,
    bFilter: true,
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
      {data: "id"},
      {data: "id", render: function (data, type, row, meta) { return appIdLink(row)}},
      {data: "name", render: function (data, type, row, meta) { return escapeHtml(data)}},
      {data: "owner", "defaultContent": "NULL"},
      {data: "proxyUser", "defaultContent": "NULL"},
      {data: "state"},
      {data: "id", render: function (data, type, row, meta) {return logLinks(row, "batch")}},
      {data: "server", "defaultContent": "NULL"},
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
