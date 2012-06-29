class Schema
	attr_reader :name, :facts, :attributes, :id

	def initialize(name, facts, attributes)
		@name = name
		@facts = facts
		@attributes = attributes
		@id = name+'_id'

		raise 'Schema name should only contain letters, digits and underscores' if name =~ /\W/
	end	

	def self.load (file_path)
		
	end
end