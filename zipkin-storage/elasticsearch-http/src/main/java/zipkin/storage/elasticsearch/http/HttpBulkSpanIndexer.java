/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.storage.elasticsearch.http;

import java.io.IOException;
import okio.Buffer;
import zipkin.Span;
import zipkin.storage.elasticsearch.InternalElasticsearchClient;

final class HttpBulkSpanIndexer extends HttpBulkIndexer<Span> implements
    InternalElasticsearchClient.BulkSpanIndexer {

  HttpBulkSpanIndexer(ElasticsearchHttpClient delegate, String spanType) {
    super(delegate, spanType, delegate.spanAdapter);
  }

  @Override public void add(String index, Span span, Long timestampMillis) throws IOException {
    String id = null; // Allow ES to choose an ID
    if (timestampMillis == null) {
      super.add(index, span, id);
      return;
    }
    writeIndexMetadata(index, id);

    // When the given timestamp isn't a field, we need to add it to the json. In this case, we're
    // doing so directly rather than hacking the json adapter.
    Buffer spanBuffer = new Buffer();
    adapter.toJson(spanBuffer, span);
    body.writeUtf8("{\"timestamp_millis\":").writeDecimalLong(timestampMillis).writeByte(',');
    spanBuffer.readByte(); // throws away the opening { in the span as we already wrote one
    spanBuffer.readAll(body);
    body.writeByte('\n');

    if (delegate.flushOnWrites) indices.add(index);
  }
}
