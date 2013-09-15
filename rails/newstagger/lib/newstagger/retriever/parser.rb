require 'nokogiri'

module NewsTagger
  module Parsers

    module Utils

      module Nokogiri::XML

        class Node
          def one_level_text?
            self.children.size == 1 and self.children.first.text?
          end
        end

      end

      def select_set_to_parse(node, selectors)
        node_set = node.css(*selectors)
        begin
          yield node_set
        ensure
          node_set.unlink
        end
      end

      # enforce select only 1 and there are only 1 to select
      def select_only_node_to_parse(node, selector, allow_empty = false)
        node_set = node.css(selector)
        raise "Only one to exist, however there are #{node_set.size}" if node_set.size > 1
        raise "There must be one node matches '#{selector}'" unless allow_empty or node_set.size == 1
        return nil if node_set.empty?
        begin
          yield node_set.first
        ensure
          node_set.unlink
        end
      end

      def ensure_empty_node(node)
        return true if node.nil?
        if node.text?
          node.unlink if node.content.strip.empty?
          return true
        end
        node.children.each do |n|
          ensure_empty_node n
        end
        if node.children.empty?
          node.unlink
          return true
        end
        raise "Node is not empty\n#{node.path}\n#{node.inspect}"
      end

      def select(node, selectors)
        sset = []
        selectors.each do |selector|
          sset << {:selector => selector, :node_set => node.css(selector)}
        end
        node_set = node.css(*selectors)
        begin
          r = []
          node_set.each do |n|
            sset.each do |s|
              if s[:node_set].include? n
                nr = yield(s[:selector], n)
                r << nr unless nr.nil?
                next
              end
            end
          end
          return nil if r.empty?
          r
        ensure
          node_set.unlink
        end
      end
    end

    module ParserRules

      class ParserRuleNotMatchException < StandardError

      end

      class RuleBase
        include Parsers::Utils
        include Parsers::ParserRules

        def parse(node_sequence, parent_node)
          HTMLParser.parse_node_set(node_sequence)
        end
      end
    end

    class HTMLParser
      include Utils

      def parse(node)
        HTMLParser.parse_node(node)
      end


      def self.parse_attributes(node)
        r = []
        node.attributes.each do |name, attr|
          r << {:name => name, :value => attr.value}
        end
        return nil if r.empty?
        r
      end

      def self.parse_node_set(node_set)
        r = []
        node_set.each do |node|
          parsed_node = parse_node(node)
          r << parsed_node unless parsed_node.nil?
        end
        return nil if r.empty?
        r
      end

      def self.parse_node(node)
        begin
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
                  :children => parse_node_set(node.children)
              }
            when Nokogiri::XML::Node::HTML_DOCUMENT_NODE
              key = :html
              r = {
                  :children => parse_node_set(node.children)
              }
            else
              return nil
          end
          r.reject! { |k, v| v.nil? }
          {key => r}
        ensure
          node.unlink
        end
      end

    end

  end
end