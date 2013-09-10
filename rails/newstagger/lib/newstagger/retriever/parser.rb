require 'nokogiri'

module NewsTagger
  module Parsers

    class HTMLParser

      def parse_attributes(node)
        r = []
        node.attributes.each do |name, attr|
          r << {:name => name, :value => attr.value}
        end
        r
      end

      def parse_node_set(node_set, selector_parser = :all, parse_remainder = :default, fail_over = false)
        r = []
        node_set.each do |node|
          parsed_node = parse(node, selector_parser, parse_remainder, fail_over)
          r << parsed_node unless parsed_node.nil?
        end
        return nil if r.empty?
        r
      end

      def parse(node, selector_parser = :all, parse_remainder = :default, fail_over = false)
        if selector_parser == :all
          key = nil
          r = {}
          case node.type
            when Nokogiri::XML::Node::CDATA_SECTION_NODE
              return {:cdata => node.content}
            when Nokogiri::XML::Node::COMMENT_NODE
              return {:comment => node.content}
            when Nokogiri::XML::Node::TEXT_NODE
              return {:text => node.content}
            when Nokogiri::XML::Node::ELEMENT_NODE
              key = :element
              r = {
                  :name => node.name,
                  :attributes => parse_attributes(node),
                  :children => parse_node_set(node.children, :all, :default, fail_over)
              }
            when Nokogiri::XML::Node::HTML_DOCUMENT_NODE
              key = :html
              r = {
                  :children => parse_node_set(node.children, :all, :default, fail_over)
              }
            else
              return nil
          end
          r.reject! { |k, v| v.nil? }
          return {key => r}
        end
        []
      end
    end

  end
end