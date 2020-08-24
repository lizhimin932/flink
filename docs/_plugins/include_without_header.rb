# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

module Jekyll
  module Tags

    class IncludeWithoutHeaderTag < Liquid::Tag

      def initialize(tag_name, text, tokens)
        super
        @file = text.strip
      end

      def render(context)
        source = File.expand_path(context.registers[:site].config['source']).freeze
        path = File.join(source, @file)
        content = File.read(path)
        content = content.split(/<!--[^>]*LICENSE-2.0[^>]*-->/, 2)[1]
        partial = Liquid::Template.parse(content)
        partial.render!(context)
      end
    end
  end
end

Liquid::Template.register_tag("include_without_header", Jekyll::Tags::IncludeWithoutHeaderTag)
