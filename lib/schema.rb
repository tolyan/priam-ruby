require "cassandra-cql"

module SchemaHelper
	#TBD
end

class CubedRecord
	attr_reader :timestamp, :attributes, :facts

	def initialize(timestamp, attributes, facts)
		@timestamp = timestamp
		@facts = facts
		@attributes = attributes.sort
	end

	def ==(another_record)
		@timestamp == another_record.timestamp and @facts == another_record.facts and @attributes == another_record.attributes

	end

	def project(column_family)

		result_attributes = @attributes.select {|k,v| column_family.attributes.include?(k.to_s)}
		result_facts = @facts.select {|k,v| column_family.fact == k.to_s}
	
		if result_attributes.length == column_family.attributes.length and result_facts.length == 1
			result = CubedRecord.new(@timestamp, result_attributes, result_facts)
		else
			false
		end
	end

	def id
		@timestamp.gsub(/-/,'')
	end

	def insert_string
		self.id + ', ' + @attributes.map{ |k,v| "'" + v.to_s + "'"}.join(', ') + ', ' + @facts.map{ |k,v| v.to_s}.first
	end
end

class CubeColumnFamily
	attr_reader :name, :attributes, :fact

	def initialize(attributes, fact)
		@attributes = attributes
		@fact = fact
		@name = attributes.join('__') + "__#{fact}"
	end

	def insert_string
		"INSERT into #{@name} (id, #{@attributes.join(', ')}, #{fact})"
	end

end

class Schema
	include SchemaHelper

	@@db = CassandraCQL::Database.new('127.0.0.1:9160')

	attr_reader :name, :facts, :attributes, :timestamp, :column_families
	
	def self.db 
		@@db
	end
	
	# INITIALIZATION

	def initialize(name, timestamp, facts=[], attributes=[], *options)
		# set init attributes
		@name = name
		@timestamp = timestamp
		@facts = facts.sort
		@attributes = attributes.sort
		@options = options

		# elaborate on schema
		@column_families = []

		attributes_combination(2).product(@facts).each do |x|
			@column_families << CubeColumnFamily.new(x[0],x[1])
		end

		raise 'Schema name should only contain letters, digits and underscores' if name =~ /\W/
		raise 'Schema timestamp should only contain letters, digits and underscores' if timestamp =~ /\W/
	end	

	def self.load (file_path)
		#TBD
	end

	# CASSANDRA SCHEMA MANAGEMENT
	def whipeout!
		@@db.execute("DROP KEYSPACE #{name}")
	end

	def dump
		unless @@db.keyspaces.map(&:name).include?(name)

			# create keyspace
			@@db.execute("CREATE KEYSPACE #{name} WITH strategy_class='org.apache.cassandra.locator.SimpleStrategy'
                AND strategy_options:replication_factor=1")

			@@db.execute("USE #{name}")

			# create column families
			@column_families.each { |cf|
				@@db.execute("CREATE COLUMNFAMILY #{cf.name} (id varchar PRIMARY KEY)")
			}
        else
            return false    
		end
	end

	def dump!
		self.whipeout!
		self.dump
	end

	# DATA INSERT
	def insert(record)
		@@db.execute("USE #{name}")
		@column_families.each { |cf|
			if insert_record = record.project(cf)
				statement = cf.insert_string + " VALUES (#{insert_record.insert_string})"
			
				@@db.execute(statement)
			end
		}
		
	end

	# HELPERS

	def attributes_combination(k = attributes.length)
		a = Array.new
		n = @attributes.length

		(k..n).each do |i|
			a = a + @attributes.combination(i).to_a
		end

		return a
	end

	
end