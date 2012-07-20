require "cassandra-cql"

module SchemaHelper
	#TBD
end

class CubeColumnFamily
	attr_reader :name, :attributes, :fact

	def initialize(attributes, fact)
		@attributes = attributes
		@fact = fact
		@name = attributes.join('__') + "__#{fact}"
	end

	def match(record)

		if @attributes == record[:attributes].map{|k,v| k.to_s}.sort and record[:facts].map{|k,v| k.to_s}.include(@fact)
			return true
		else
			return false
		end
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
	def insert(timestamp, attributes, facts)
		@@db.execute(INSERT)
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